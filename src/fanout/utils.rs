use crate::fanout::FanoutTargetMode;
use crate::{
    Context, ContextFlags, RedisModule_CachedMicroseconds, RedisModule_GetSelectedDb,
    RedisModule_Milliseconds, RedisModule_SelectDb, Status, ValkeyResult,
};
use std::sync::atomic::AtomicBool;

const VALKEYMODULE_CLIENT_INFO_FLAG_READONLY: u64 = 1 << 6; /* Valkey 9 */

pub static FORCE_REPLICAS_READONLY: AtomicBool = AtomicBool::new(false);

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

/// Helper function to check if the Valkey server version is considered "legacy" (e.g., < 9).
/// In legacy versions, client read-only status might not be reliably determinable.
fn is_valkey_version_legacy(context: &Context) -> bool {
    context
        .get_server_version()
        .is_ok_and(|version| version.major < 9)
}

pub fn compute_query_fanout_mode(context: &Context) -> FanoutTargetMode {
    #[cfg(test)]
    if FORCE_REPLICAS_READONLY.load(std::sync::atomic::Ordering::Relaxed) {
        // Testing only
        return FanoutTargetMode::ReplicasOnly;
    }

    // Determine fanout mode based on Valkey version and client read-only status.
    // The following logic is based on the issue https://github.com/valkey-io/valkey-search/issues/139
    if is_valkey_version_legacy(context) {
        // Valkey 8 doesn't provide a way to determine if a client is READONLY,
        // So we choose random distribution.
        FanoutTargetMode::Random
    } else {
        match is_client_read_only(context) {
            Ok(true) => FanoutTargetMode::Random,
            Ok(false) => FanoutTargetMode::Primary,
            Err(_) => {
                // If we can't determine client read-only status, default to Random
                log::warn!(
                    "Could not determine client read-only status, defaulting to Random fanout mode."
                );
                FanoutTargetMode::Random
            }
        }
    }
}

/// Returns the time duration since UNIX_EPOCH in milliseconds.
fn system_time_millis() -> i64 {
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