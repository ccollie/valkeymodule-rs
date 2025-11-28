mod cluster_rpc;
mod fanout_operation;
mod serialization;
mod utils;
mod encoding;
mod fanout_error;
mod cluster_map;
mod blocked_client;
mod registry;
mod fanout_message;

use arc_swap::{ArcSwap, Guard};
use std::sync::{Arc, LazyLock};
use std::sync::atomic::AtomicU64;

pub use serialization::*;
pub use utils::*;
pub use fanout_error::*;
pub use fanout_operation::*;
use crate::Context;

use super::fanout::cluster_rpc::register_cluster_message_handlers;

pub use cluster_map::{
    ClusterMap, 
    FanoutTargetMode, 
    NodeId,
    NodeInfo, 
    NodeRole, 
    NodeLocation,
    ShardInfo,
    SocketAddress,
    CURRENT_NODE_ID
};

pub use registry::{
    register_fanout_operation,
    get_registered_fanout_operations,
};

pub static CLUSTER_MAP_EXPIRATION_MS: AtomicU64 = AtomicU64::new(3000); // secs

pub(crate) fn init_fanout(ctx: &Context) {
    register_cluster_message_handlers(ctx);
}

static CLUSTER_MAP: LazyLock<ArcSwap<ClusterMap>> =
    LazyLock::new(|| ArcSwap::from_pointee(ClusterMap::default()));

pub fn get_cluster_map() -> Guard<Arc<ClusterMap>> {
    CLUSTER_MAP.load()
}

fn update_cluster_map(map: ClusterMap) {
    CLUSTER_MAP.swap(Arc::new(map));
}

pub fn get_fanout_targets(ctx: &Context, mode: FanoutTargetMode) -> Arc<Vec<NodeInfo>> {
    let current_map = CLUSTER_MAP.load();
    // Check if we need to refresh
    let needs_refresh = !current_map.is_consistent || current_map.is_expired();
    if !needs_refresh {
        return current_map.get_targets(mode);
    }
    // Possibly race condition, but only if called concurrently, which is possible but very unlikely.
    // In any case, the worst that can happen is that we refresh more than once.
    refresh_cluster_map(ctx);
    CLUSTER_MAP.load().get_targets(mode)
}

// Refresh the cluster map by creating a new one from the current cluster state
pub fn refresh_cluster_map(ctx: &Context) {
    log::info!("Refreshing cluster map...");
    let new_map = ClusterMap::create(ctx);
    log::info!(
        "Updating cluster map with new map, is_full={}",
        new_map.is_cluster_map_full
    );
    update_cluster_map(new_map);
    log::info!("Cluster map refreshed");
}

pub fn get_or_refresh_cluster_map(ctx: &Context) -> Arc<ClusterMap> {
    let current_map = get_cluster_map();

    // Check if we need to refresh
    let needs_refresh = !current_map.is_consistent || current_map.is_expired();

    if needs_refresh {
        drop(current_map);
        refresh_cluster_map(ctx);
        return get_cluster_map().clone();
    }

    current_map.clone()
}
