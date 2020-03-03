#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate rocket;
#[macro_use]
extern crate rocket_contrib;
#[macro_use]
extern crate serde_derive;

pub mod api;
pub mod skill_base;
pub mod store;

mod merge;
mod message;
mod player;
mod true_skill;
