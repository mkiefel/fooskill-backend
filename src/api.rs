use rocket::{
    get,
    http::Status,
    post,
    request::Request,
    response::{self, Responder},
    serde::{json::Json, Deserialize, Serialize},
    State,
};
use rocket_db_pools::Connection;

use crate::merge;
use crate::message::Message;
use crate::skill_base::{self, decode_and_validate_group_id, Error, GameId, UserId};
use crate::store::Store;

impl<'r> rocket::request::FromParam<'r> for UserId {
    type Error = &'r str;

    fn from_param(param: &'r str) -> Result<Self, Self::Error> {
        Ok(UserId::from(param.to_string()))
    }
}

impl<'r> Responder<'r, 'static> for Error {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        match self {
            Error::UserAlreadyExists => Err(Status::Conflict),
            Error::UserNameTooShort => Err(Status::BadRequest),
            Error::Merge(merge::Error::MissingEntryError(_)) => Err(Status::NotFound),
            Error::InvalidGroupId => Err(Status::BadRequest),
            err => {
                println!("{:?}", err);
                Err(Status::InternalServerError)
            }
        }
    }
}

#[derive(Serialize, Debug)]
struct Game {
    id: GameId,
    winner_ids: Vec<UserId>,
    loser_ids: Vec<UserId>,
    timestamp: u128,
}

impl From<skill_base::Game> for Game {
    fn from(game: skill_base::Game) -> Self {
        Game {
            id: game.id().clone(),
            winner_ids: game.winner_ids().clone(),
            loser_ids: game.loser_ids().clone(),
            timestamp: game.datetime().naive_utc().timestamp_millis() as u128,
        }
    }
}

#[derive(Serialize, Debug)]
struct User {
    id: UserId,
    name: String,
    player: Player,
}

#[derive(Serialize, Debug)]
struct Player {
    skill: Message,
}

impl From<skill_base::User> for User {
    fn from(user: skill_base::User) -> Self {
        User {
            id: user.id().clone(),
            name: user.name().to_owned(),
            player: Player {
                skill: user.player().skill_at(&chrono::Utc::now()).unwrap(),
            },
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct PostGameRequest {
    winner_ids: Vec<UserId>,
    loser_ids: Vec<UserId>,
}

#[derive(Serialize, Debug)]
pub struct PostGameResponse {
    game: Game,
}

#[derive(Deserialize)]
pub struct GroupKeyConfig {
    pub group_key: skill_base::GroupKey,
}

#[post("/<secret_group_id>/games", data = "<request>")]
pub async fn post_game(
    mut store: Connection<Store>,
    group_key_config: &State<GroupKeyConfig>,
    secret_group_id: String,
    request: Json<PostGameRequest>,
) -> Result<Json<PostGameResponse>, Error> {
    let group_id = decode_and_validate_group_id(&group_key_config.group_key, secret_group_id)?;
    let game_id = GameId::from(uuid::Uuid::new_v4().simple().to_string());
    skill_base::create_game(
        &mut store,
        &group_id,
        &game_id,
        &request.winner_ids,
        &request.loser_ids,
        chrono::Utc::now(),
    )
    .await
    .map(|game| Json(PostGameResponse { game: game.into() }))
}

#[derive(Serialize, Debug)]
pub struct GetGamesResponse {
    games: Vec<Game>,
}

#[get("/<secret_group_id>/games?<before>")]
pub async fn get_games(
    mut store: Connection<Store>,
    group_key_config: &State<GroupKeyConfig>,
    secret_group_id: String,
    before: Option<GameId>,
) -> Result<Json<GetGamesResponse>, Error> {
    let group_id = decode_and_validate_group_id(&group_key_config.group_key, secret_group_id)?;
    skill_base::list_games(&mut store, &group_id, &before)
        .await
        .map(|games| {
            Json(GetGamesResponse {
                games: games.into_iter().map(Game::from).collect(),
            })
        })
}

#[derive(Deserialize, Debug)]
pub struct PostUserRequest {
    name: String,
}

#[derive(Serialize, Debug)]
pub struct PostUserResponse {
    user: User,
}

#[post("/<secret_group_id>/users", data = "<request>")]
pub async fn post_user(
    mut store: Connection<Store>,
    group_key_config: &State<GroupKeyConfig>,
    secret_group_id: String,
    request: Json<PostUserRequest>,
) -> Result<Json<PostUserResponse>, Error> {
    let group_id = decode_and_validate_group_id(&group_key_config.group_key, secret_group_id)?;
    let user_id = UserId::from(uuid::Uuid::new_v4().simple().to_string());
    skill_base::create_user(&mut store, &group_id, &user_id, &request.name)
        .await
        .map(|user| Json(PostUserResponse { user: user.into() }))
}

#[derive(Serialize, Debug)]
pub struct GetUserResponse {
    user: User,
}

#[get("/<secret_group_id>/users/<user_id>")]
pub async fn get_user(
    mut store: Connection<Store>,
    group_key_config: &State<GroupKeyConfig>,
    secret_group_id: String,
    user_id: UserId,
) -> Result<Json<GetUserResponse>, Error> {
    let group_id = decode_and_validate_group_id(&group_key_config.group_key, secret_group_id)?;
    skill_base::read_users(&mut store, &group_id, &[user_id])
        .await
        .map(|mut users| {
            let user = users.pop().unwrap();
            Json(GetUserResponse { user: user.into() })
        })
}

#[derive(Serialize, Debug)]
pub struct QueryUserResponse {
    query: String,
    users: Vec<User>,
}

#[get("/<secret_group_id>/users?<query>")]
pub async fn query_user(
    mut store: Connection<Store>,
    group_key_config: &State<GroupKeyConfig>,
    secret_group_id: String,
    query: String,
) -> Result<Json<QueryUserResponse>, Error> {
    let group_id = decode_and_validate_group_id(&group_key_config.group_key, secret_group_id)?;
    skill_base::query_user(&mut store, &group_id, &query)
        .await
        .map(|users| {
            Json(QueryUserResponse {
                query,
                users: users.into_iter().map(User::from).collect(),
            })
        })
}

#[derive(Serialize, Debug)]
pub struct GetLeaderboardResponse {
    users: Vec<User>,
}

#[get("/<secret_group_id>/leaderboard")]
pub async fn get_leaderboard(
    mut store: Connection<Store>,
    group_key_config: &State<GroupKeyConfig>,
    secret_group_id: String,
) -> Result<Json<GetLeaderboardResponse>, Error> {
    let group_id = decode_and_validate_group_id(&group_key_config.group_key, secret_group_id)?;
    skill_base::get_leaderboard(&mut store, &group_id, &chrono::Utc::now())
        .await
        .map(|users| {
            Json(GetLeaderboardResponse {
                users: users.into_iter().map(User::from).collect(),
            })
        })
}
