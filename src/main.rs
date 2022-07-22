use std::collections::HashMap;

use rocket::fs::FileServer;
use rocket::{fairing::AdHoc, get, launch, routes};
use rocket_db_pools::{Connection, Database};
use rocket_dyn_templates::Template;
use serde::Serialize;

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
async fn user(
    mut store: Connection<Store>,
    group_key_config: &rocket::State<api::GroupKeyConfig>,
    secret_group_id: String,
    user_id: skill_base::UserId,
) -> Result<Template, skill_base::Error> {
    let group_id = skill_base::decode_and_validate_group_id(
        &group_key_config.group_key,
        secret_group_id.clone(),
    )?;
    let users = skill_base::read_users(&mut store, &group_id, &[user_id.clone()]).await?;
    let user = users.first().unwrap();
    let games = skill_base::get_recent_games(&mut store, &group_id, &user_id).await?;

    let mut joined_games = Vec::new();
    for game in games {
        let winners =
            skill_base::read_users(&mut store, &group_id, &game.clone().winner_ids()).await?;
        let losers =
            skill_base::read_users(&mut store, &group_id, &game.clone().loser_ids()).await?;
        joined_games.push(JoinedGame { winners, losers });
    }
    let context = GetUserContext {
        secret_group_id: percent_encoding::utf8_percent_encode(
            &secret_group_id,
            percent_encoding::NON_ALPHANUMERIC,
        )
        .to_string(),
        user: user.clone(),
        games: joined_games,
    };
    Ok(Template::render("user", &context))
}

#[get("/<_secret_group_id>")]
fn group(_secret_group_id: String) -> Template {
    let context: HashMap<String, String> = HashMap::new();
    Template::render("index", context)
}

#[get("/")]
fn index() -> Template {
    let context: HashMap<String, String> = HashMap::new();
    Template::render("index", context)
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .attach(AdHoc::config::<api::GroupKeyConfig>())
        .attach(Store::init())
        .attach(Template::fairing())
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
        .mount("/static", FileServer::from("frontend/static"))
}
