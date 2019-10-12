#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate rocket;
#[macro_use]
extern crate rocket_contrib;
#[macro_use]
extern crate serde_derive;

mod api;
mod merge;
mod store;
mod true_skill;

use std::collections::HashMap;

use rocket_contrib::serve::{Options, StaticFiles};
use rocket_contrib::templates;

use store::Store;

/// Repesents a game with all players being resolved to their user data.
#[derive(Serialize, Debug)]
struct JoinedGame {
    winners: Vec<store::User>,
    losers: Vec<store::User>,
}

/// Used to render the user detail page.
#[derive(Serialize, Debug)]
struct GetUserContext {
    secret_group_id: String,
    user: store::User,
    games: Vec<JoinedGame>,
}

#[get("/<secret_group_id>/users/<user_id>")]
fn user(
    mut store: Store,
    group_key: rocket::State<store::GroupKey>,
    secret_group_id: String,
    user_id: store::UserId,
) -> Result<templates::Template, store::Error> {
    let group_id = store::decode_and_validate_group_id(&group_key, secret_group_id.clone())?;
    let users = store.read_users(&group_id, &vec![user_id.clone()])?;
    let user = users.first().unwrap();
    let games = store.get_recent_games(&group_id, &user_id)?;
    let joined_games = games
        .iter()
        .map(|game: &store::Game| -> Result<JoinedGame, store::Error> {
            let winners = store.read_users(&group_id, &game.winner_ids)?;
            let losers = store.read_users(&group_id, &game.loser_ids)?;
            Ok(JoinedGame { winners, losers })
        })
        .collect::<Result<Vec<JoinedGame>, _>>()?;
    let context = GetUserContext {
        secret_group_id: percent_encoding::utf8_percent_encode(
            &secret_group_id,
            percent_encoding::NON_ALPHANUMERIC,
        )
        .to_string(),
        user: user.clone(),
        games: joined_games,
    };
    Ok(templates::Template::render("user", &context))
}

#[get("/<_secret_group_id>")]
fn group(_secret_group_id: String) -> templates::Template {
    let context: HashMap<String, String> = HashMap::new();
    templates::Template::render("index", context)
}

#[get("/")]
fn index() -> templates::Template {
    let context: HashMap<String, String> = HashMap::new();
    templates::Template::render("index", context)
}

fn main() {
    rocket::ignite()
        .mount(
            "/api/v1.0/",
            routes![
                api::get_leaderboard,
                api::get_user,
                api::query_user,
                api::post_user,
                api::get_games,
                api::post_game,
            ],
        )
        .mount("/", routes![index, group, user])
        .mount(
            "/static",
            StaticFiles::new("frontend/static", Options::None),
        )
        .attach(rocket::fairing::AdHoc::on_attach(
            "Group Key Config",
            |rocket| {
                let maybe_key = rocket
                    .config()
                    .get_str("group_key")
                    .ok()
                    .and_then(|key| store::GroupKey::new(key.to_owned()));

                match maybe_key {
                    Some(key) => Ok(rocket.manage(key)),
                    None => Ok(rocket),
                }
            },
        ))
        .attach(Store::fairing())
        .attach(templates::Template::fairing())
        .launch();
}
