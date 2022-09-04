use rocket::fs::{FileServer, NamedFile};
use rocket::{fairing::AdHoc, get, launch, routes};
use rocket_db_pools::Database;

use fooskill::api;
use fooskill::store::Store;

#[get("/<_..>", rank = 100)]
async fn index() -> Option<NamedFile> {
    NamedFile::open("frontend/static/index.html").await.ok()
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .attach(AdHoc::config::<api::GroupKeyConfig>())
        .attach(Store::init())
        .mount(
            "/api/v1.0/",
            routes![
                api::get_leaderboard,
                api::get_user,
                api::get_user_games,
                api::query_user,
                api::post_user,
                api::get_games,
                api::post_game,
            ],
        )
        .mount("/static", FileServer::from("frontend/static"))
        .mount("/", routes![index])
}
