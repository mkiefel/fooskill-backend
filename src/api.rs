use rocket_contrib::json::Json;

use crate::store::{decode_and_validate_group_id, Error, GameId, GroupKey, Store, UserId};
use crate::player::Player;

#[derive(Serialize, Debug)]
struct Game {
    id: GameId,
    winner_ids: Vec<UserId>,
    loser_ids: Vec<UserId>,
}

impl From<crate::store::Game> for Game {
    fn from(game: crate::store::Game) -> Self {
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

impl From<crate::store::User> for User {
    fn from(user: crate::store::User) -> Self {
        User {
            id: user.id().clone(),
            name: user.name().to_owned(),
            player: user.player().clone(),
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
    store
        .create_game(
            &group_id,
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
    store.list_games(&group_id, &before).map(|games| {
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
    store
        .create_user(&group_id, &request.name)
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
    store.read_users(&group_id, &[user_id]).map(|mut users| {
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
    store.query_user(&group_id, &query).map(|users| {
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
    store.get_leaderboard(&group_id).map(|users| {
        Json(GetLeaderboardResponse {
            users: users.into_iter().map(User::from).collect(),
        })
    })
}
