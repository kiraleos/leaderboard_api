use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct User {
    pub _id: bson::oid::ObjectId,
    country: String,
    avatar: String,
    age: i32,
    name: String,
    #[serde(skip_deserializing)]
    pub score: u32,
}
