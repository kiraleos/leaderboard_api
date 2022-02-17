mod model;

use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use bson::{doc, Document};
use chrono::{Timelike, Utc};
use futures::stream::TryStreamExt;
use redis::Commands;
use serde::Serialize;
use std::collections::HashMap;

use model::User;

#[derive(Serialize, Debug, Default)]
struct Response {
    total_users: u64,
    time_to_hour: String,
    leaderboard: Vec<User>,
    source: Source,
    response_time: String,
}

#[derive(Serialize, Debug)]
enum Source {
    MongoDB,
    Cache,
    Default,
}
impl Default for Source {
    fn default() -> Self {
        Source::Default
    }
}

#[get("/api/leaderboard/{page}")]
async fn get_leaderboard(page: web::Path<u32>) -> impl Responder {
    let t_start = std::time::Instant::now();
    let page = page.into_inner();

    let mut con = redis::Client::open("REDIS_INSTANCE_URL")
        .unwrap()
        .get_connection()
        .unwrap();

    let timestamp = (Utc::now() + chrono::Duration::hours(2))
        .format("%Y%m%d-%H")
        .to_string();

    let id_with_scores: Vec<(String, u32)> = con
        .zrevrange_withscores(
            timestamp,
            25 * (page - 1) as isize,
            ((25 * page) - 1) as isize,
        )
        .unwrap();

    let client_options =
        mongodb::options::ClientOptions::parse("MONGODB_URL")
            .await
            .unwrap();

    let user_collection = mongodb::Client::with_options(client_options)
        .unwrap()
        .database("db_name")
        .collection::<Document>("users");

    let mut local_con = redis::Client::open("redis://localhost:6379")
        .unwrap()
        .get_connection()
        .unwrap();

    let all_users_string: String =
        local_con.get("users").unwrap_or_default();
    let mut all_users: HashMap<String, User> =
        serde_json::from_str(&all_users_string).unwrap_or_default();

    let users_count: u64;
    let res: Response;
    let time_to_hour = chrono::Duration::minutes(60).num_minutes()
        - (chrono::Utc::now().minute()) as i64;
    let mut scores: Vec<User> = Vec::with_capacity(id_with_scores.len());
    let source: Source;

    if all_users.is_empty() {
        all_users = user_collection
            .find(None, None)
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap()
            .into_iter()
            .map(|doc: Document| bson::from_document(doc).unwrap())
            .into_iter()
            .map(|user: User| (user._id.to_string(), user))
            .collect();

        let _: () = local_con
            .set("users", serde_json::to_string(&all_users).unwrap())
            .unwrap();
        let _: () = local_con
            .expire("users", 60 * time_to_hour as usize)
            .unwrap();

        users_count =
            user_collection.count_documents(None, None).await.unwrap();

        let _: () = local_con.set("users_count", users_count).unwrap();
        let _: () = local_con
            .expire("users_count", 60 * time_to_hour as usize)
            .unwrap();

        source = Source::MongoDB;
    } else {
        users_count = local_con.get("users_count").unwrap();
        source = Source::Cache;
    }

    for (id, score) in id_with_scores {
        let user = all_users.get_mut(&id).unwrap();
        user.score = score;
        scores.push(user.clone());
    }

    res = Response {
        leaderboard: scores,
        total_users: users_count,
        source,
        time_to_hour: time_to_hour.to_string() + " minutes",
        response_time: t_start.elapsed().as_millis().to_string() + "ms",
    };

    HttpResponse::Ok()
        .append_header(("Content-Type", "application/json"))
        .body(serde_json::to_string_pretty(&res).unwrap())
}
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| App::new().service(get_leaderboard))
        .bind("127.0.0.1:3000")?
        .run()
        .await
}
