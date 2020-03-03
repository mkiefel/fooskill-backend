use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::{Deref, DerefMut};

use derive_more::From;
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

#[derive(Clone)]
pub struct GroupId(String);
#[derive(Clone, Debug, From, Serialize, Deserialize)]
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
        .map(|cookie| GroupId(cookie.value().to_owned()))
        .ok_or(Error::InvalidGroupId)
}

#[derive(Serialize, Clone, Deserialize, Debug)]
enum StoredUser {
    V0(User),
}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct User {
    id: UserId,
    name: String,
    player: Player,
}

impl StoredUser {
    fn wrap(inner: User) -> Self {
        StoredUser::V0(inner)
    }

    fn latest(self) -> User {
        match self {
            StoredUser::V0(user) => user,
        }
    }
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
enum StoredGame {
    V0(GameV0),
    V1(Game),
}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct GameV0 {
    id: GameId,
    timestamp: u128,
    winner_ids: Vec<UserId>,
    loser_ids: Vec<UserId>,
}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct Game {
    id: GameId,
    datetime: chrono::DateTime<chrono::Utc>,
    winner_ids: Vec<UserId>,
    loser_ids: Vec<UserId>,
}

impl StoredGame {
    fn wrap(inner: Game) -> Self {
        StoredGame::V1(inner)
    }

    fn latest(self) -> Game {
        match self {
            StoredGame::V0(game) => Game {
                id: game.id,
                datetime: chrono::DateTime::<chrono::Utc>::from_utc(
                    chrono::NaiveDateTime::from_timestamp(
                        (game.timestamp / 1000).try_into().unwrap(),
                        (game.timestamp % 1000 * 1_000_000).try_into().unwrap(),
                    ),
                    chrono::Utc,
                ),
                winner_ids: game.winner_ids,
                loser_ids: game.loser_ids,
            },
            StoredGame::V1(game) => game,
        }
    }
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
    C: redis::ConnectionLike,
{
    con: &'a mut C,
    group_id: GroupId,
    cache: HashMap<UserId, merge::StoredMergeable<UserId, StoredUser>>,
}

impl<'a, C> UserStoreCtx<'a, C>
where
    C: redis::ConnectionLike,
{
    fn append(&self, pipe: &mut redis::Pipeline) {
        for (k, v) in self.cache.iter() {
            pipe.set(Store::user_key(&self.group_id, &k), RedisJson(v));
        }
    }
}

impl<'a, C> merge::MergeCtx for UserStoreCtx<'a, C>
where
    C: redis::ConnectionLike,
{
    type Index = UserId;
    type Item = StoredUser;

    fn get_node(
        &mut self,
        index: &Self::Index,
    ) -> Option<merge::StoredMergeable<Self::Index, Self::Item>> {
        // First check if this key is already in our local read cache.
        if let Some(cache_item) = self.cache.get(index) {
            return Some(cache_item.clone());
        }
        // Up to this point we have never encountered this node, let's fetch it
        // then from the store.
        let user_key = Store::user_key(&self.group_id, index);
        redis::cmd("WATCH").arg(&user_key).query(self.con).ok()?;
        self.con
            .get(&user_key)
            .map(
                |RedisJson::<merge::StoredMergeable<Self::Index, Self::Item>>(node)| {
                    // Insert into cache for the next lookup.
                    self.cache.insert(index.clone(), node.clone());
                    node
                },
            )
            .ok()
    }

    fn set_node(
        &mut self,
        index: &Self::Index,
        item: merge::StoredMergeable<Self::Index, Self::Item>,
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
            let splits = entry.split(':').collect::<Vec<_>>();
            if splits.len() >= 2 {
                user_ids.push(UserId((*splits.last().unwrap()).to_string()));
            }
        }
        Ok(user_ids)
    }

    /// Reads all users given by a vector of user IDs.
    pub fn read_users(
        &mut self,
        group_id: &GroupId,
        user_ids: &[UserId],
    ) -> Result<Vec<User>, Error> {
        commit(self.con(), |con, pipe| {
            let mut ctx = UserStoreCtx {
                con,
                group_id: group_id.clone(),
                cache: HashMap::new(),
            };
            let users = user_ids
                .iter()
                .map(|user_id| {
                    merge::find(user_id.clone())
                        .run(&mut ctx)
                        .map(|versioned| versioned.latest())
                })
                .collect::<Result<Vec<User>, _>>();
            ctx.append(pipe);
            users
        })
    }

    /// Creates a user with the given name.
    pub fn create_user(&mut self, group_id: &GroupId, name: &str) -> Result<User, Error> {
        if name.len() < 3 {
            return Err(Error::UserNameTooShort);
        }
        let user_id = UserId(uuid::Uuid::new_v4().simple().to_string());
        let key = Self::user_key(group_id, &user_id);
        let index_entry = name.to_owned() + ":" + &user_id.0;

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
                player: Default::default(),
            };
            // TODO(mkiefel): Move this into the merge logic.
            let node: merge::StoredMergeable<UserId, StoredUser> =
                merge::StoredMergeable::new(user_id.clone(), StoredUser::wrap(user.clone()));
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
        let user_ids: Vec<UserId> =
            self.con()
                .zrevrange(Self::user_player_skill_score_key(group_id), 0, 100)?;
        // Users never will be deleted, so there is no race here.
        self.read_users(&group_id, &user_ids)
    }

    /// Reads all games given by the vector of game IDs.
    pub fn read_games(
        &mut self,
        group_id: &GroupId,
        game_ids: &[GameId],
    ) -> Result<Vec<Game>, Error> {
        game_ids
            .iter()
            .map(|game_id| {
                self.con()
                    .get(Self::game_key(group_id, &game_id))
                    .map(|RedisJson::<StoredGame>(versioned)| versioned.latest())
            })
            .collect::<Result<_, _>>()
            .map_err(|err| err.into())
    }

    /// List all games.
    ///
    /// # Arguments
    ///
    /// * `group_id` ID of the group.
    /// * `before_game_id` start listing games before this optional game ID.
    pub fn list_games(
        &mut self,
        group_id: &GroupId,
        before_game_id: &Option<GameId>,
    ) -> Result<Vec<Game>, Error> {
        let games_key = Self::games_key(group_id);
        let game_ids = commit(self.con(), |con, _pipe| -> Result<Vec<GameId>, Error> {
            let before_game_rank = before_game_id
                .as_ref()
                .map(|game_id| -> Result<isize, Error> {
                    let (rank,): (isize,) = redis::pipe()
                        .cmd("WATCH")
                        .arg(&games_key)
                        .ignore()
                        .zrevrank(&games_key, game_id.0.clone())
                        .query(con)?;
                    Ok(rank + 1)
                })
                .unwrap_or(Ok(0))?;

            con.zrevrange(
                Self::games_key(group_id),
                before_game_rank,
                before_game_rank + 99,
            )
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
    /// * `datetime` when did the game take place.
    pub fn create_game(
        &mut self,
        group_id: &GroupId,
        winner_ids: &[UserId],
        loser_ids: &[UserId],
        datetime: chrono::DateTime<chrono::Utc>,
    ) -> Result<Game, Error> {
        let game_id = GameId(uuid::Uuid::new_v4().simple().to_string());
        let key = Self::game_key(group_id, &game_id);
        let game = Game {
            id: game_id,
            datetime,
            winner_ids: winner_ids.to_owned(),
            loser_ids: loser_ids.to_owned(),
        };

        let timestamp_key = format!("{}", game.datetime.naive_utc().timestamp_millis());

        commit(
            self.con(),
            |con, pipe| -> Result<(), merge::Error<UserId>> {
                // TODO(mkiefel): Remove some of the code duplication in this
                // lambda.
                let mut ctx = UserStoreCtx {
                    con,
                    group_id: group_id.clone(),
                    cache: HashMap::new(),
                };
                let mut winners = winner_ids
                    .iter()
                    .map(|user_id| {
                        merge::find(user_id.clone())
                            .run(&mut ctx)
                            .map(|user| user.latest())
                    })
                    .collect::<Result<Vec<User>, _>>()?;
                let mut losers = loser_ids
                    .iter()
                    .map(|user_id| {
                        merge::find(user_id.clone())
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
                    merge::set(winner.id.clone(), StoredUser::wrap(winner.clone()))
                        .run(&mut ctx)?;
                    pipe.zadd(
                        Self::user_games_key(group_id, &winner.id),
                        &game.id.0,
                        &timestamp_key,
                    );
                }
                for (loser, update) in losers.iter_mut().zip(loser_updates) {
                    loser.player.skill = loser.player.skill.include(&update);
                    merge::set(loser.id.clone(), StoredUser::wrap(loser.clone())).run(&mut ctx)?;
                    pipe.zadd(
                        Self::user_games_key(group_id, &loser.id),
                        &game.id.0,
                        &timestamp_key,
                    );
                }
                let scores = winners
                    .iter()
                    .chain(losers.iter())
                    .map(|user| (Self::map_score(user), user.id.0.clone()))
                    .collect::<Vec<_>>();

                ctx.append(pipe);
                pipe.set(&key, RedisJson(StoredGame::wrap(game.clone())))
                    .zadd(Self::games_key(group_id), &game.id.0, &timestamp_key)
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
        "group:".to_owned() + &group_id.0
    }

    fn user_player_skill_score_key(group_id: &GroupId) -> String {
        Self::group_key_prefix(group_id) + ":user.player.skill.score"
    }

    fn user_name_index_key(group_id: &GroupId) -> String {
        Self::group_key_prefix(group_id) + ":user.name.index"
    }

    fn user_key(group_id: &GroupId, user_id: &UserId) -> String {
        Self::group_key_prefix(group_id) + ":user:" + &user_id.0
    }

    fn user_games_key(group_id: &GroupId, user_id: &UserId) -> String {
        Self::group_key_prefix(group_id) + ":user.games:" + &user_id.0
    }

    fn game_key(group_id: &GroupId, game_id: &GameId) -> String {
        Self::group_key_prefix(group_id) + ":game:" + &game_id.0
    }

    fn games_key(group_id: &GroupId) -> String {
        Self::group_key_prefix(group_id) + ":games"
    }
}
