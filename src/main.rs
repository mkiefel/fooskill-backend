#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;
#[macro_use]
extern crate serde_derive;

use std::collections::HashMap;

use rocket_contrib::serve::{Options, StaticFiles};
use rocket_contrib::templates;

use fooskill::api;
use fooskill::skill_base;
use fooskill::store::Store;

/// Repesents a game with all players being resolved to their user data.
#[derive(Serialize, Debug)]
struct JoinedGame {
    winners: Vec<skill_base::User>,
    losers: Vec<skill_base::User>,
}

/// Used to render the user detail page.
#[derive(Serialize, Debug)]
struct GetUserContext {
    secret_group_id: String,
    user: skill_base::User,
    games: Vec<JoinedGame>,
}

#[get("/<secret_group_id>/users/<user_id>")]
fn user(
    mut store: Store,
    group_key: rocket::State<skill_base::GroupKey>,
    secret_group_id: String,
    user_id: skill_base::UserId,
) -> Result<templates::Template, skill_base::Error> {
    let group_id = skill_base::decode_and_validate_group_id(&group_key, secret_group_id.clone())?;
    let users = skill_base::read_users(&mut store, &group_id, &[user_id.clone()])?;
    let user = users.first().unwrap();
    let games = skill_base::get_recent_games(&mut store, &group_id, &user_id)?;
    let joined_games = games
        .iter()
        .map(
            |game: &skill_base::Game| -> Result<JoinedGame, skill_base::Error> {
                let winners =
                    skill_base::read_users(&mut store, &group_id, &game.clone().winner_ids())?;
                let losers =
                    skill_base::read_users(&mut store, &group_id, &game.clone().loser_ids())?;
                Ok(JoinedGame { winners, losers })
            },
        )
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
                    .and_then(|key| skill_base::GroupKey::new(key.to_owned()));

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
