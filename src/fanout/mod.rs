mod cluster_message;
mod cluster_rpc;
mod fanout_operation;
mod fanout_targets;
mod serialization;
mod utils;
mod encoding;
mod fanout_error;
mod snowflake;

pub use serialization::*;
pub use utils::*;
pub use fanout_error::*;
pub use fanout_targets::*;
pub use cluster_rpc::*;
pub use fanout_operation::*;
use crate::Context;

pub fn init_fanout(ctx: &Context) {
    register_cluster_message_handlers(ctx)
}