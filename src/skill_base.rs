use std::cmp::PartialOrd;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use async_trait::async_trait;
use derive_more::From;
use quick_error::quick_error;
use rocket::form::FromForm;
use rocket::serde::{Deserialize, Serialize};
use rocket_db_pools::deadpool_redis::{
    redis::{self, AsyncCommands},
    Connection,
};

use crate::merge;
use crate::player::Player;
use crate::true_skill::{GameResult, TrueSkill};

#[derive(Clone)]
pub struct GroupId(String);
#[derive(Clone, From, Debug, Serialize, Deserialize, FromForm)]
pub struct GameId(String);
#[derive(Clone, Debug, PartialEq, Eq, From, Serialize, Deserialize, Hash)]
pub struct UserId(String);

impl redis::FromRedisValue for GameId {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<GameId> {
        match *v {
            redis::Value::Data(ref bytes) => Ok(GameId(std::str::from_utf8(bytes)?.to_string())),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Response was of incompatible type",
                format!("Response type not compatible. (response was {:?})", v),
            ))),
        }
    }
}

// TODO(mkiefel): Remove the copied implementation of this newtype.
impl redis::FromRedisValue for UserId {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<UserId> {
        match *v {
            redis::Value::Data(ref bytes) => Ok(UserId(std::str::from_utf8(bytes)?.to_string())),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Response was of incompatible type",
                format!("Response type not compatible. (response was {:?})", v),
            ))),
        }
    }
}

pub struct GroupKey(cookie::Key);

impl GroupKey {
    pub fn new(encoded: String) -> Option<Self> {
        base64::decode(&encoded)
            .ok()
            .map(|bytes| GroupKey(cookie::Key::derive_from(&bytes)))
    }
}

struct GroupKeyVisitor;

impl<'de> serde::de::Visitor<'de> for GroupKeyVisitor {
    type Value = GroupKey;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a cookie secret")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        GroupKey::new(s.to_owned()).ok_or(serde::de::Error::invalid_value(
            serde::de::Unexpected::Str(s),
            &self,
        ))
    }
}

impl<'de> serde::de::Deserialize<'de> for GroupKey {
    fn deserialize<D>(deserializer: D) -> Result<GroupKey, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_str(GroupKeyVisitor)
    }
}

pub fn decode_and_validate_group_id(
    group_key: &GroupKey,
    secret_group_id: String,
) -> Result<GroupId, Error> {
    let mut jar = cookie::CookieJar::new();
    jar.add(cookie::Cookie::build("group_id", secret_group_id).finish());
    let private = jar.private(&group_key.0);
    private
        .get("group_id")
        .map(|cookie| GroupId(cookie.value().to_owned()))
        .ok_or(Error::InvalidGroupId)
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Redis(err: redis::RedisError) {
            cause(err)
                from()
        }
        Merge(err: merge::Error<UserId>) {
            cause(err)
                from()
        }
        UserAlreadyExists {}
        UserNameTooShort {}
        InvalidGroupId {}
    }
}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct User {
    id: UserId,
    name: String,
    player: Player,
}

impl User {
    pub fn id(&self) -> &UserId {
        &self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn player(&self) -> &Player {
        &self.player
    }
}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct Game {
    id: GameId,
    datetime: chrono::DateTime<chrono::Utc>,
    winner_ids: Vec<UserId>,
    loser_ids: Vec<UserId>,
}

impl Game {
    pub fn winner_ids(&self) -> &Vec<UserId> {
        &self.winner_ids
    }

    pub fn loser_ids(&self) -> &Vec<UserId> {
        &self.loser_ids
    }

    pub fn id(&self) -> &GameId {
        &self.id
    }

    pub fn datetime(&self) -> &chrono::DateTime<chrono::Utc> {
        &self.datetime
    }
}

#[derive(Debug)]
struct RedisJson<T>(T);

impl<T: serde::de::DeserializeOwned> redis::FromRedisValue for RedisJson<T> {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<RedisJson<T>> {
        match *v {
            redis::Value::Data(ref bytes) => serde_json::from_slice::<T>(bytes)
                .map(RedisJson)
                .map_err(|error| {
                    redis::RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Response was of incompatible type",
                        format!(
                            "Response type not JSON compatible: {:?} (response was {:?})",
                            error.to_string(),
                            v
                        ),
                    ))
                }),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Response was of incompatible type",
                format!("Response type not compatible. (response was {:?})", v),
            ))),
        }
    }
}

impl<T: serde::Serialize> redis::ToRedisArgs for RedisJson<T> {
    fn write_redis_args<W: ?Sized>(&self, out: &mut W)
    where
        W: redis::RedisWrite,
    {
        out.write_arg(&serde_json::to_vec(&self.0).unwrap())
    }
}

impl<T> Deref for RedisJson<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for RedisJson<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

struct UserStoreCtx<'a, C>
where
    C: redis::aio::ConnectionLike,
{
    con: &'a mut C,
    group_id: GroupId,
    cache: HashMap<UserId, merge::Mergeable<UserId, User>>,
}

impl<'a, C> UserStoreCtx<'a, C>
where
    C: redis::aio::ConnectionLike,
{
    fn append(&self, pipe: &mut redis::Pipeline) {
        for (k, v) in self.cache.iter() {
            pipe.set(user_key(&self.group_id, &k), RedisJson(v));
        }
    }
}

#[async_trait]
impl<'a, C> merge::MergeCtx for UserStoreCtx<'a, C>
where
    C: redis::aio::ConnectionLike + std::marker::Send,
{
    type Index = UserId;
    type Item = User;

    async fn get_node(
        &mut self,
        index: &Self::Index,
    ) -> Option<merge::Mergeable<Self::Index, Self::Item>> {
        // First check if this key is already in our local read cache.
        if let Some(cache_item) = self.cache.get(index) {
            return Some(cache_item.clone());
        }
        // Up to this point we have never encountered this node, let's fetch it
        // then from the store.
        let user_key = user_key(&self.group_id, index);

        redis::cmd("WATCH")
            .arg(&user_key)
            .query_async(self.con)
            .await
            .ok()?;

        self.con
            .get(&user_key)
            .await
            .map(
                |RedisJson::<merge::Mergeable<Self::Index, Self::Item>>(node)| {
                    // Insert into cache for the next lookup.
                    self.cache.insert(index.clone(), node.clone());
                    node
                },
            )
            .ok()
    }

    async fn set_node(
        &mut self,
        index: &Self::Index,
        item: merge::Mergeable<Self::Index, Self::Item>,
    ) {
        // The value is just set in the cache. Only when the transaction is
        // committed, it will be written to the store.
        self.cache.insert(index.clone(), item);
    }
}

/// Commit a single transaction to the data store. Returns the results of the
/// successfully committed transaction.
///
/// # Arguments
///
/// * `con` the Redis connection.
/// * `f` the transaction to commit.
macro_rules! commit {
    ($con:expr, $pipe_name:ident, $body:expr) => {{
        let return_value: Result<_, Error> = loop {
            let mut $pipe_name = redis::pipe();
            $pipe_name.atomic();

            // Return early if there was something strange while building up the transaction.
            let result: Result<_, Error> = async { $body }.await;
            if let Err(_) = result {
                break result;
            }

            // Wait for the transaction to finish.
            let transaction: Option<()> = $pipe_name.query_async($con).await?;

            if let Some(()) = transaction {
                // The transaction finish successfully. Let's return the result from the above
                // body. This assumes that we are not interested in the result of the trnasaction
                // itsself as it's meant to only write information.
                break result;
            }
        };
        return_value
    }};
}

async fn query_user_index(
    con: &mut Connection,
    group_id: &GroupId,
    query: &str,
) -> Result<Vec<UserId>, Error> {
    let entries: Vec<String> = con
        .zrangebylex_limit(
            user_name_index_key(group_id),
            "[".to_owned() + query,
            "[".to_owned() + query + std::str::from_utf8(&[0x7f_u8]).unwrap(),
            0,
            10,
        )
        .await?;

    let mut user_ids = Vec::new();
    for entry in entries {
        let splits = entry.split(':').collect::<Vec<_>>();
        if splits.len() >= 2 {
            user_ids.push(UserId((*splits.last().unwrap()).to_string()));
        }
    }
    Ok(user_ids)
}

/// Reads all users given by a vector of user IDs.
pub async fn read_users(
    con: &mut Connection,
    group_id: &GroupId,
    user_ids: &[UserId],
) -> Result<Vec<User>, Error> {
    commit!(&mut *con, pipe, {
        let mut ctx = UserStoreCtx {
            con,
            group_id: group_id.clone(),
            cache: HashMap::new(),
        };
        let mut users = Vec::new();
        for user_id in user_ids {
            users.push(merge::find(&mut ctx, user_id.clone()).await?);
        }

        ctx.append(&mut pipe);
        Ok(users)
    })
}

/// Creates a user with the given name.
///
/// If a user with the same ID already exists, it will be overwritten.
///
/// # Arguments
///
/// * `group_id` user will belong to this group.
/// * `user_id` user will have this ID.
/// * `name` of the user.
pub async fn create_user(
    con: &mut Connection,
    group_id: &GroupId,
    user_id: &UserId,
    name: &str,
) -> Result<User, Error> {
    if name.len() < 3 {
        return Err(Error::UserNameTooShort);
    }
    let key = user_key(group_id, user_id);
    let index_entry = name.to_owned() + ":" + &user_id.0;

    let user_name_index = user_name_index_key(group_id);
    commit!(&mut *con, pipe, {
        // Verify that the user does yet exist.
        redis::cmd("WATCH").arg(&key).query_async(con).await?;
        let entries: Vec<String> = con
            .zrangebylex_limit(
                &user_name_index,
                "[".to_owned() + name + ":",
                "[".to_owned() + name + ":" + std::str::from_utf8(&[0x7f_u8]).unwrap(),
                0,
                1,
            )
            .await?;
        if !entries.is_empty() {
            return Err(Error::UserAlreadyExists);
        }

        let user = User {
            id: user_id.to_owned(),
            name: name.to_owned(),
            player: Default::default(),
        };
        // TODO(mkiefel): Move this into the merge logic.
        let node: merge::Mergeable<UserId, User> =
            merge::Mergeable::new(user_id.clone(), user.clone());
        pipe.set(&key, RedisJson(&node))
            .ignore()
            .zadd(&user_name_index, index_entry.clone(), 0_f32)
            .ignore()
            .sadd(user_id_key(group_id), &user_id.0)
            .ignore();
        Ok(user)
    })
}

/// Reads the last 100 games from a user.
pub async fn get_recent_games(
    con: &mut Connection,
    group_id: &GroupId,
    user_id: &UserId,
) -> Result<Vec<Game>, Error> {
    // TODO(mkiefel): Implement some form of pagination for this.
    let game_ids: Vec<GameId> = con
        .zrevrange(user_games_key(group_id, user_id), 0, 100)
        .await?;
    // Games never will be deleted, so there is no race here.
    read_games(con, group_id, &game_ids).await
}

/// Finds users whose name match the query.
pub async fn query_user(
    con: &mut Connection,
    group_id: &GroupId,
    query: &str,
) -> Result<Vec<User>, Error> {
    // TODO(mkiefel): Implement some form of pagination for this.
    let user_ids = query_user_index(con, group_id, query).await?;
    // Users never will be deleted, so there is no race here.
    read_users(con, group_id, &user_ids).await
}

/// Reads the top 100 users.
pub async fn get_leaderboard(
    con: &mut Connection,
    group_id: &GroupId,
    datetime: &chrono::DateTime<chrono::Utc>,
) -> Result<Vec<User>, Error> {
    // TODO(mkiefel): Implement some form of pagination for this.
    let user_ids: Vec<UserId> = con.smembers(user_id_key(group_id)).await?;
    // Users never will be deleted, so there is no race here.
    let mut users = read_users(con, &group_id, &user_ids).await?;
    users.sort_unstable_by(|user_a, user_b| {
        let score_a = -map_score(user_a, datetime);
        let score_b = -map_score(user_b, datetime);
        score_a.partial_cmp(&score_b).unwrap()
    });
    Ok(users)
}

/// Reads all games given by the vector of game IDs.
pub async fn read_games(
    con: &mut Connection,
    group_id: &GroupId,
    game_ids: &[GameId],
) -> Result<Vec<Game>, Error> {
    Ok(con
        .get::<Vec<String>, Vec<RedisJson<Game>>>(
            game_ids
                .iter()
                .map(|game_id| game_key(group_id, &game_id))
                .collect::<Vec<_>>(),
        )
        .await?
        .into_iter()
        .map(|RedisJson::<Game>(game)| game)
        .collect())
}

/// List all games.
///
/// # Arguments
///
/// * `group_id` ID of the group.
/// * `before_game_id` start listing games before this optional game ID.
pub async fn list_games(
    con: &mut Connection,
    group_id: &GroupId,
    before_game_id: &Option<GameId>,
) -> Result<Vec<Game>, Error> {
    let games_key = games_key(group_id);
    let game_ids: Vec<GameId> = commit!(&mut *con, pipe, {
        let before_game_rank = if let Some(game_id) = before_game_id {
            let (_, rank): ((), isize) = redis::pipe()
                .cmd("WATCH")
                .arg(&games_key)
                .ignore()
                .zrevrank(&games_key, game_id.0.clone())
                .query_async(con)
                .await?;
            rank + 1
        } else {
            0
        };

        con.zrevrange(&games_key, before_game_rank, before_game_rank + 99)
            .await
            .map_err(|err| err.into())
    })?;
    // Games never will be deleted, so there is no race here.
    read_games(con, group_id, &game_ids).await
}

/// Create a game and update all involved player scores.
///
/// If a game with the same ID already exists, it will be overwritten.
///
/// # Arguments
///
/// * `group_id` ID of the group.
/// * `game_id` ID of the game to create.
/// * `winner_ids` user IDs of winning users.
/// * `loser_ids` user IDs of losing users.
/// * `datetime` when did the game take place.
pub async fn create_game(
    con: &mut Connection,
    group_id: &GroupId,
    game_id: &GameId,
    winner_ids: &[UserId],
    loser_ids: &[UserId],
    datetime: chrono::DateTime<chrono::Utc>,
) -> Result<Game, Error> {
    let key = game_key(group_id, &game_id);
    let game = Game {
        id: game_id.clone(),
        datetime,
        winner_ids: winner_ids.to_owned(),
        loser_ids: loser_ids.to_owned(),
    };

    let timestamp_key = format!("{}", game.datetime.naive_utc().timestamp_millis());

    commit!(&mut *con, pipe, {
        // TODO(mkiefel): a lot of the users can be fetched in parallel.
        let mut ctx = UserStoreCtx {
            con,
            group_id: group_id.clone(),
            cache: HashMap::new(),
        };
        // Get user stats.
        let mut winners = Vec::new();
        for winner_id in winner_ids {
            winners.push(merge::find(&mut ctx, winner_id.clone()).await?);
        }
        let mut losers = Vec::new();
        for loser_id in loser_ids {
            losers.push(merge::find(&mut ctx, loser_id.clone()).await?);
        }

        // Reason about skills.
        let true_skill = TrueSkill::new(Player::default_sigma() / 2.0, 0.0);
        let (winner_updates, loser_updates) = true_skill.tree_pass(
            &winners
                .iter()
                .map(|user| user.player.skill_at(&datetime).unwrap())
                .collect::<Vec<_>>(),
            &losers
                .iter()
                .map(|user| user.player.skill_at(&datetime).unwrap())
                .collect::<Vec<_>>(),
            GameResult::Won,
        );

        // Update user stats.
        for (winner, update) in winners.iter_mut().zip(winner_updates) {
            winner.player.set_skill(
                winner.player.skill_at(&datetime).unwrap().include(&update),
                datetime,
            );
            merge::set(&mut ctx, winner.id.clone(), winner.clone()).await?;
            pipe.zadd(
                user_games_key(group_id, &winner.id),
                &game.id.0,
                &timestamp_key,
            );
        }
        for (loser, update) in losers.iter_mut().zip(loser_updates) {
            loser.player.set_skill(
                loser.player.skill_at(&datetime).unwrap().include(&update),
                datetime,
            );
            merge::set(&mut ctx, loser.id.clone(), loser.clone()).await?;
            pipe.zadd(
                user_games_key(group_id, &loser.id),
                &game.id.0,
                &timestamp_key,
            );
        }

        ctx.append(&mut pipe);
        pipe.set(&key, RedisJson(game.clone())).zadd(
            games_key(group_id),
            &game.id.0,
            &timestamp_key,
        );
        Ok(())
    })?;
    Ok(game)
}

fn map_score(user: &User, datetime: &chrono::DateTime<chrono::Utc>) -> f64 {
    let (mu, sigma2) = user.player.skill_at(datetime).unwrap().to_mu_sigma2();
    mu - 2.0 * sigma2.sqrt()
}

fn group_key_prefix(group_id: &GroupId) -> String {
    "group:".to_owned() + &group_id.0
}

fn user_id_key(group_id: &GroupId) -> String {
    group_key_prefix(group_id) + ":user.id"
}

fn user_name_index_key(group_id: &GroupId) -> String {
    group_key_prefix(group_id) + ":user.name.index"
}

fn user_key(group_id: &GroupId, user_id: &UserId) -> String {
    group_key_prefix(group_id) + ":user:" + &user_id.0
}

fn user_games_key(group_id: &GroupId, user_id: &UserId) -> String {
    group_key_prefix(group_id) + ":user.games:" + &user_id.0
}

fn game_key(group_id: &GroupId, game_id: &GameId) -> String {
    group_key_prefix(group_id) + ":game:" + &game_id.0
}

fn games_key(group_id: &GroupId) -> String {
    group_key_prefix(group_id) + ":games"
}
