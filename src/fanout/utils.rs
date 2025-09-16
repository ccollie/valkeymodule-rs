use super::fanout_targets::{get_cluster_node_info};
use super::snowflake::SnowflakeIdGenerator;
use crate::{
    Context, ContextFlags, DetachedContext, RedisModule_Milliseconds, ValkeyModule_GetMyClusterID, ValkeyResult,
};
use std::hash::{BuildHasher, RandomState};
use std::net::Ipv6Addr;
use std::os::raw::c_char;
use std::sync::LazyLock;

const VALKEYMODULE_CLIENT_INFO_FLAG_READONLY: u64 = 1 << 6; /* Valkey 9 */

pub fn is_client_read_only(ctx: &Context) -> ValkeyResult<bool> {
    let info = ctx.get_client_info()?;
    Ok(info.flags & VALKEYMODULE_CLIENT_INFO_FLAG_READONLY != 0)
}

pub fn is_clustered(ctx: &Context) -> bool {
    let flags = ctx.get_flags();
    flags.contains(ContextFlags::CLUSTER)
}

pub fn is_multi_or_lua(ctx: &Context) -> bool {
    let flags = ctx.get_flags();
    flags.contains(ContextFlags::MULTI) || flags.contains(ContextFlags::LUA)
}

/// Returns the time duration since UNIX_EPOCH in milliseconds.
fn system_time_millis() -> i64 {
    // TODO: use a more efficient way to get current time
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as i64
}

fn valkey_current_time_millis() -> i64 {
    unsafe { RedisModule_Milliseconds.unwrap()() }
}

pub(super) fn current_time_millis() -> i64 {
    cfg_if::cfg_if! {
        if #[cfg(test)] {
            system_time_millis()
        } else {
            valkey_current_time_millis()
        }
    }
}

pub fn get_current_node_ip(ctx: &Context) -> Option<Ipv6Addr> {
    unsafe {
        // C API: Get current node's cluster ID
        let node_id = ValkeyModule_GetMyClusterID
            .expect("ValkeyModule_GetMyClusterID function is unavailable")();

        if node_id.is_null() {
            // We're not clustered, so a random string is good enough
            return Some(Ipv6Addr::LOCALHOST);
        }
        let Some(info) = get_cluster_node_info(ctx, node_id) else {
            // todo: log error
            ctx.log_warning("get_current_node_ip(): error getting cluster node info");
            return None;
        };
        Some(info.addr())
    }
}


static ID_GENERATOR: LazyLock<SnowflakeIdGenerator> = LazyLock::new(|| {
    let ctx = DetachedContext::new();
    let addr = get_current_node_ip(&ctx.lock()).unwrap_or(Ipv6Addr::LOCALHOST);
    let hasher = RandomState::new();
    let hash = hasher.hash_one(addr);
    let node_id: u32 = hash as u32;
    let machine_id = (hash >> 32) as u32;
    SnowflakeIdGenerator::new(machine_id, node_id)
});

pub(super) fn generate_id() -> u64 {
    ID_GENERATOR.generate()
}
