use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::time::{SystemTime, UNIX_EPOCH};

use redis::{self, Commands, PipelineCommands};
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{self, Responder};
use rocket_contrib::databases::{r2d2, DatabaseConfig, DbError, Poolable};
use transaction::Transaction;

use crate::merge;
use crate::true_skill::{GameResult, Player, TrueSkill};

// Thin wrapper around r2d2_redis to implement Poolable for a newer version of
// redis-rs.
pub struct Connection(redis::Connection);
pub struct ConnectionManager(r2d2_redis::RedisConnectionManager);

impl Connection {
    fn con(&mut self) -> &mut redis::Connection {
        &mut self.0
    }
}

impl r2d2::ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = r2d2_redis::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.0.connect().map(Connection)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        self.0.is_valid(&mut conn.0)
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        self.0.has_broken(&mut conn.0)
    }
}

impl Poolable for Connection {
    type Manager = ConnectionManager;
    type Error = DbError<redis::RedisError>;

    fn pool(config: DatabaseConfig) -> Result<r2d2::Pool<Self::Manager>, Self::Error> {
        let manager =
            r2d2_redis::RedisConnectionManager::new(config.url).map_err(DbError::Custom)?;
        r2d2::Pool::builder()
            .max_size(config.pool_size)
            .build(ConnectionManager(manager))
            .map_err(DbError::PoolError)
    }
}

#[database("fooskill")]
pub struct Store(Connection);

pub type GroupId = String;
pub type GameId = String;
pub type UserId = String;

pub struct GroupKey(cookie::Key);

impl GroupKey {
    pub fn new(encoded: String) -> Option<Self> {
        base64::decode(&encoded)
            .ok()
            .map(|bytes| GroupKey(cookie::Key::from_master(&bytes)))
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
        .map(|cookie| cookie.value().to_owned())
        .ok_or(Error::InvalidGroupId)
}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub enum VersionedUser {
    V0(User),
}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct User {
    pub id: UserId,
    pub name: String,
    pub player: Player,
}

impl VersionedUser {
    fn new(user: User) -> Self {
        VersionedUser::V0(user)
    }

    fn latest(self) -> User {
        match self {
            VersionedUser::V0(user) => user,
        }
    }
}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub enum VersionedGame {
    V0(Game),
}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct Game {
    pub id: GameId,
    pub timestamp: u128,
    pub winner_ids: Vec<UserId>,
    pub loser_ids: Vec<UserId>,
}

impl VersionedGame {
    fn new(game: Game) -> Self {
        VersionedGame::V0(game)
    }

    fn latest(self) -> Game {
        match self {
            VersionedGame::V0(game) => game,
        }
    }
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

impl<'r> Responder<'r> for Error {
    fn respond_to(self, _: &Request) -> response::Result<'r> {
        match self {
            Error::UserAlreadyExists => Err(Status::Conflict),
            Error::UserNameTooShort => Err(Status::BadRequest),
            Error::Merge(merge::Error::MissingEntryError(_)) => Err(Status::NotFound),
            Error::InvalidGroupId => Err(Status::BadRequest),
            _ => Err(Status::InternalServerError),
        }
    }
}

#[derive(Debug)]
struct RedisJson<T>(T);

impl<T: serde::de::DeserializeOwned> redis::FromRedisValue for RedisJson<T> {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<RedisJson<T>> {
        match *v {
            redis::Value::Data(ref bytes) => serde_json::from_slice::<T>(bytes)
                .map(|value| RedisJson(value))
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
    C: redis::ConnectionLike,
{
    con: &'a mut C,
    cache: HashMap<UserId, merge::Mergeable<UserId, VersionedUser>>,
}

impl<'a, C> UserStoreCtx<'a, C>
where
    C: redis::ConnectionLike,
{
    fn append(&self, pipe: &mut redis::Pipeline) {
        for (k, v) in self.cache.iter() {
            pipe.set(k, RedisJson(v));
        }
    }
}

impl<'a, C> merge::MergeCtx for UserStoreCtx<'a, C>
where
    C: redis::ConnectionLike,
{
    type Index = UserId;
    type Item = VersionedUser;

    fn get_node(
        &mut self,
        index: &Self::Index,
    ) -> Option<merge::Mergeable<Self::Index, Self::Item>> {
        // First check if this key is already in our local read cache.
        if let Some(cache_item) = self.cache.get(index) {
            return Some(cache_item.clone());
        }
        // Up to this point we have never encountered this node, let's fetch it
        // then from the store.
        redis::cmd("WATCH")
            .arg(index.clone())
            .query(self.con)
            .ok()?;
        self.con
            .get(index.clone())
            .map(
                |RedisJson::<merge::Mergeable<Self::Index, Self::Item>>(node)| {
                    // Insert into cache for the next lookup.
                    self.cache.insert(index.clone(), node.clone());
                    node
                },
            )
            .ok()
    }

    fn set_node(&mut self, index: &Self::Index, item: merge::Mergeable<Self::Index, Self::Item>) {
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
fn commit<C, F, R, E>(con: &mut C, f: F) -> Result<R, Error>
where
    C: redis::ConnectionLike,
    F: Fn(&mut C, &mut redis::Pipeline) -> Result<R, E>,
    E: std::convert::Into<Error>,
{
    loop {
        let mut pipe = redis::pipe();
        pipe.atomic();
        let r = f(con, &mut pipe).map_err(|err| err.into())?;
        if let Some(()) = pipe.query(con)? {
            return Ok(r);
        }
        // Commit was not successful so try again.
    }
}

impl Store {
    fn query_user_index(&mut self, group_id: &GroupId, query: &str) -> Result<Vec<UserId>, Error> {
        let entries: Vec<String> = self.con().zrangebylex_limit(
            Self::user_name_index_key(group_id),
            "[".to_owned() + query,
            "[".to_owned() + query + std::str::from_utf8(&[0x7f_u8]).unwrap(),
            0,
            10,
        )?;

        let mut user_ids = Vec::new();
        for entry in entries {
            let splits = entry.split(":").collect::<Vec<_>>();
            if splits.len() >= 2 {
                user_ids.push(String::from(*splits.last().unwrap()));
            }
        }
        Ok(user_ids)
    }

    /// Reads all users given by a vector of user IDs.
    pub fn read_users(
        &mut self,
        group_id: &GroupId,
        user_ids: &Vec<UserId>,
    ) -> Result<Vec<User>, Error> {
        commit(self.con(), |con, pipe| {
            let mut ctx = UserStoreCtx {
                con,
                cache: HashMap::new(),
            };
            let users = user_ids
                .iter()
                .map(|user_id| {
                    merge::find(Self::user_key(group_id, user_id))
                        .run(&mut ctx)
                        .map(|user| user.latest())
                })
                .collect::<Result<Vec<User>, _>>();
            ctx.append(pipe);
            users
        })
        .map_err(|err| err.into())
    }

    /// Creates a user with the given name.
    pub fn create_user(&mut self, group_id: &GroupId, name: &str) -> Result<User, Error> {
        if name.len() < 3 {
            return Err(Error::UserNameTooShort);
        }
        let user_id = uuid::Uuid::new_v4().simple().to_string();
        let key = Self::user_key(group_id, &user_id);
        let index_entry = name.to_owned() + ":" + &user_id;

        let user_name_index = Self::user_name_index_key(group_id);
        commit(self.con(), |con, pipe| {
            // Verify that the user does yet exist.
            redis::cmd("WATCH").arg(&key).query(con)?;
            let entries: Vec<String> = con.zrangebylex_limit(
                &user_name_index,
                "[".to_owned() + name + ":",
                "[".to_owned() + name + ":" + std::str::from_utf8(&[0x7f_u8]).unwrap(),
                0,
                1,
            )?;
            if !entries.is_empty() {
                return Err(Error::UserAlreadyExists);
            }

            let user = User {
                id: user_id.clone(),
                name: name.to_owned(),
                player: crate::true_skill::Player::new(),
            };
            let node: merge::Mergeable<UserId, VersionedUser> =
                merge::Mergeable::new(key.clone(), VersionedUser::new(user.clone()));
            pipe.set(&key, RedisJson(&node))
                .ignore()
                .zadd(&user_name_index, index_entry.clone(), 0_f32)
                .ignore()
                .query(con)?;
            Ok(user)
        })
    }

    /// Reads the last 100 games from a user.
    pub fn get_recent_games(
        &mut self,
        group_id: &GroupId,
        user_id: &UserId,
    ) -> Result<Vec<Game>, Error> {
        // TODO(mkiefel): Implement some form of pagination for this.
        let game_ids: Vec<GameId> =
            self.con()
                .zrevrange(Self::user_games_key(group_id, user_id), 0, 100)?;
        // Games never will be deleted, so there is no race here.
        self.read_games(group_id, &game_ids)
    }

    /// Finds users whose name match the query.
    pub fn query_user(&mut self, group_id: &GroupId, query: &str) -> Result<Vec<User>, Error> {
        // TODO(mkiefel): Implement some form of pagination for this.
        let user_ids = self.query_user_index(group_id, query)?;
        // Users never will be deleted, so there is no race here.
        self.read_users(group_id, &user_ids)
    }

    /// Reads the top 100 users.
    pub fn get_leaderboard(&mut self, group_id: &GroupId) -> Result<Vec<User>, Error> {
        // TODO(mkiefel): Implement some form of pagination for this.
        let user_ids: Vec<String> =
            self.con()
                .zrevrange(Self::user_player_skill_score_key(group_id), 0, 100)?;
        // Users never will be deleted, so there is no race here.
        self.read_users(&group_id, &user_ids)
    }

    /// Reads all games given by the vector of game IDs.
    pub fn read_games(
        &mut self,
        group_id: &GroupId,
        game_ids: &Vec<GameId>,
    ) -> Result<Vec<Game>, Error> {
        game_ids
            .iter()
            .map(|game_id| {
                self.con()
                    .get(Self::game_key(group_id, &game_id))
                    .map(|RedisJson::<VersionedGame>(versioned_game)| versioned_game.latest())
            })
            .collect::<Result<_, _>>()
            .map_err(|err| err.into())
    }

    /// List all games.
    ///
    /// # Arguments
    ///
    /// * `group_id` ID of the group.
    /// * `after_game_id` start listing games after this optional game ID.
    pub fn list_games(
        &mut self,
        group_id: &GroupId,
        after_game_id: &Option<GameId>,
    ) -> Result<Vec<Game>, Error> {
        let games_key = Self::games_key(group_id);
        let game_ids = commit(self.con(), |con, _pipe| -> Result<Vec<GameId>, Error> {
            let after_game_rank = after_game_id
                .as_ref()
                .map(|game_id| -> Result<isize, Error> {
                    let (rank,): (isize,) = redis::pipe()
                        .cmd("WATCH")
                        .arg(&games_key)
                        .ignore()
                        .zrank(&games_key, game_id)
                        .query(con)?;
                    Ok(rank + 1)
                })
                .unwrap_or(Ok(0))?;

            con.zrange(Self::games_key(group_id), after_game_rank, 100)
                .map_err(|err| err.into())
        })?;
        // Games never will be deleted, so there is no race here.
        self.read_games(group_id, &game_ids)
    }

    /// Create a game and update all involved player scores.
    ///
    /// # Arguments
    ///
    /// * `group_id` ID of the group.
    /// * `winner_ids` user IDs of winning users.
    /// * `loser_ids` user IDs of losing users.
    pub fn create_game(
        &mut self,
        group_id: &GroupId,
        winner_ids: &Vec<UserId>,
        loser_ids: &Vec<UserId>,
    ) -> Result<Game, Error> {
        let game_id = uuid::Uuid::new_v4().simple().to_string();
        let key = Self::game_key(group_id, &game_id);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("backwards?")
            .as_millis();
        let game = Game {
            id: game_id,
            timestamp: timestamp,
            winner_ids: winner_ids.clone(),
            loser_ids: loser_ids.clone(),
        };

        let timestamp_str = format!("{}", timestamp);

        commit(
            self.con(),
            |con, pipe| -> Result<(), merge::Error<UserId>> {
                // TODO(mkiefel): Remove some of the code duplication in this
                // lambda.
                let mut ctx = UserStoreCtx {
                    con,
                    cache: HashMap::new(),
                };
                let mut winners = winner_ids
                    .iter()
                    .map(|user_id| {
                        merge::find(Self::user_key(group_id, user_id))
                            .run(&mut ctx)
                            .map(|user| user.latest())
                    })
                    .collect::<Result<Vec<User>, _>>()?;
                let mut losers = loser_ids
                    .iter()
                    .map(|user_id| {
                        merge::find(Self::user_key(group_id, user_id))
                            .run(&mut ctx)
                            .map(|user| user.latest())
                    })
                    .collect::<Result<Vec<User>, _>>()?;

                let true_skill = TrueSkill::new(Player::default_sigma());
                let (winner_updates, loser_updates) = true_skill.tree_pass(
                    &winners
                        .iter()
                        .map(|user| user.player.skill)
                        .collect::<Vec<_>>(),
                    &losers
                        .iter()
                        .map(|user| user.player.skill)
                        .collect::<Vec<_>>(),
                    GameResult::Won,
                );
                for (winner, update) in winners.iter_mut().zip(winner_updates) {
                    winner.player.skill = winner.player.skill.include(&update);
                    merge::set(
                        Self::user_key(group_id, &winner.id),
                        VersionedUser::new(winner.clone()),
                    )
                    .run(&mut ctx)?;
                    pipe.zadd(
                        Self::user_games_key(group_id, &winner.id),
                        &game.id,
                        &timestamp_str,
                    );
                }
                for (loser, update) in losers.iter_mut().zip(loser_updates) {
                    loser.player.skill = loser.player.skill.include(&update);
                    merge::set(
                        Self::user_key(group_id, &loser.id),
                        VersionedUser::new(loser.clone()),
                    )
                    .run(&mut ctx)?;
                    pipe.zadd(
                        Self::user_games_key(group_id, &loser.id),
                        &game.id,
                        &timestamp_str,
                    );
                }
                let scores = winners
                    .iter()
                    .chain(losers.iter())
                    .map(|user| (Self::map_score(user), user.id.to_owned()))
                    .collect::<Vec<_>>();

                ctx.append(pipe);
                pipe.set(&key, RedisJson(VersionedGame::new(game.clone())))
                    .zadd(Self::games_key(group_id), &game.id, &timestamp_str)
                    .zadd_multiple(Self::user_player_skill_score_key(group_id), &scores);
                Ok(())
            },
        )?;
        Ok(game)
    }

    fn map_score(user: &User) -> f64 {
        let (mu, sigma2) = user.player.skill.to_mu_sigma2();
        mu - 2.0 * sigma2.sqrt()
    }

    fn group_key_prefix(group_id: &GroupId) -> String {
        "group:".to_owned() + group_id
    }

    fn user_player_skill_score_key(group_id: &GroupId) -> String {
        Self::group_key_prefix(group_id) + ":user.player.skill.score"
    }

    fn user_name_index_key(group_id: &GroupId) -> String {
        Self::group_key_prefix(group_id) + ":user.name.index"
    }

    fn user_key(group_id: &GroupId, user_id: &UserId) -> String {
        Self::group_key_prefix(group_id) + ":user:" + user_id
    }

    fn user_games_key(group_id: &GroupId, user_id: &UserId) -> String {
        Self::group_key_prefix(group_id) + ":user.games:" + user_id
    }

    fn game_key(group_id: &GroupId, game_id: &GameId) -> String {
        Self::group_key_prefix(group_id) + ":game:" + game_id
    }

    fn games_key(group_id: &GroupId) -> String {
        Self::group_key_prefix(group_id) + ":games"
    }
}
