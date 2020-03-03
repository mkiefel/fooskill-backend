use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{self, Responder};
use rocket_contrib::json::Json;

use crate::merge;
use crate::message::Message;
use crate::skill_base::{self, decode_and_validate_group_id, Error, GameId, GroupKey, UserId};
use crate::store::Store;

impl<'r> rocket::request::FromParam<'r> for UserId {
    type Error = &'r rocket::http::RawStr;

    fn from_param(param: &'r rocket::http::RawStr) -> Result<Self, Self::Error> {
        param
            .percent_decode()
            .map(|cow| cow.into_owned().into())
            .map_err(|_| param)
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

#[derive(Serialize, Debug)]
struct Game {
    id: GameId,
    winner_ids: Vec<UserId>,
    loser_ids: Vec<UserId>,
}

impl From<skill_base::Game> for Game {
    fn from(game: skill_base::Game) -> Self {
        Game {
            id: game.id().clone(),
            winner_ids: game.winner_ids().clone(),
            loser_ids: game.loser_ids().clone(),
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

impl<'a> rocket::request::FromFormValue<'a> for GameId {
    type Error = &'a rocket::http::RawStr;

    fn from_form_value(form_value: &'a rocket::http::RawStr) -> Result<Self, Self::Error> {
        form_value
            .url_decode()
            .map(Into::<GameId>::into)
            .map_err(|_| form_value)
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

#[post("/<secret_group_id>/games", data = "<request>")]
pub fn post_game(
    mut store: Store,
    group_key: rocket::State<GroupKey>,
    secret_group_id: String,
    request: Json<PostGameRequest>,
) -> Result<Json<PostGameResponse>, Error> {
    let group_id = decode_and_validate_group_id(&group_key, secret_group_id)?;
    let game_id = GameId::from(uuid::Uuid::new_v4().simple().to_string());
    skill_base::create_game(
        &mut store,
        &group_id,
        &game_id,
        &request.winner_ids,
        &request.loser_ids,
        chrono::Utc::now(),
    )
    .map(|game| Json(PostGameResponse { game: game.into() }))
}

#[derive(Serialize, Debug)]
pub struct GetGamesResponse {
    games: Vec<Game>,
}

#[get("/<secret_group_id>/games?<before>")]
pub fn get_games(
    mut store: Store,
    group_key: rocket::State<GroupKey>,
    secret_group_id: String,
    before: Option<GameId>,
) -> Result<Json<GetGamesResponse>, Error> {
    let group_id = decode_and_validate_group_id(&group_key, secret_group_id)?;
    skill_base::list_games(&mut store, &group_id, &before).map(|games| {
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
pub fn post_user(
    mut store: Store,
    group_key: rocket::State<GroupKey>,
    secret_group_id: String,
    request: Json<PostUserRequest>,
) -> Result<Json<PostUserResponse>, Error> {
    let group_id = decode_and_validate_group_id(&group_key, secret_group_id)?;
    let user_id = UserId::from(uuid::Uuid::new_v4().simple().to_string());
    skill_base::create_user(&mut store, &group_id, &user_id, &request.name)
        .map(|user| Json(PostUserResponse { user: user.into() }))
}

#[derive(Serialize, Debug)]
pub struct GetUserResponse {
    user: User,
}

#[get("/<secret_group_id>/users/<user_id>")]
pub fn get_user(
    mut store: Store,
    group_key: rocket::State<GroupKey>,
    secret_group_id: String,
    user_id: UserId,
) -> Result<Json<GetUserResponse>, Error> {
    let group_id = decode_and_validate_group_id(&group_key, secret_group_id)?;
    skill_base::read_users(&mut store, &group_id, &[user_id]).map(|mut users| {
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
pub fn query_user(
    mut store: Store,
    group_key: rocket::State<GroupKey>,
    secret_group_id: String,
    query: String,
) -> Result<Json<QueryUserResponse>, Error> {
    let group_id = decode_and_validate_group_id(&group_key, secret_group_id)?;
    skill_base::query_user(&mut store, &group_id, &query).map(|users| {
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
pub fn get_leaderboard(
    mut store: Store,
    group_key: rocket::State<GroupKey>,
    secret_group_id: String,
) -> Result<Json<GetLeaderboardResponse>, Error> {
    let group_id = decode_and_validate_group_id(&group_key, secret_group_id)?;
    skill_base::get_leaderboard(&mut store, &group_id, &chrono::Utc::now()).map(|users| {
        Json(GetLeaderboardResponse {
            users: users.into_iter().map(User::from).collect(),
        })
    })
}
