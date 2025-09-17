use crate::{
    Context, 
    ValkeyModuleCtx,
    ValkeyModule_GetClusterNodeInfo,
    ValkeyModule_GetClusterNodesList, REDISMODULE_NODE_MASTER, VALKEYMODULE_NODE_FAIL,
    VALKEYMODULE_NODE_ID_LEN, VALKEYMODULE_NODE_MYSELF, VALKEYMODULE_NODE_PFAIL, VALKEYMODULE_OK,
};
use rand::prelude::*;
use rand::rng;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::net::Ipv6Addr;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use crate::fanout::{is_client_read_only};

pub static FORCE_REPLICAS_READONLY: AtomicBool = AtomicBool::new(false);

// master_id and ip are not null terminated, so we add 1 for null terminator for safety
pub(super) const MASTER_ID_LEN: usize = (VALKEYMODULE_NODE_ID_LEN + 1) as usize;

/// Maximum length of an IPv6 address string
pub const INET6_ADDR_STR_LEN: usize = 46;

pub type ClusterNodeIdBuf = [u8; (VALKEYMODULE_NODE_ID_LEN as usize) + 1]; // +1 for null terminator
pub(super) type ClusterNodeMap = HashMap<String, Vec<ClusterNodeInfo>>;

/// Enumeration for fanout target modes
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum FanoutTargetMode {
    #[default]
    Random, // Default: randomly select one node per shard
    Primary, // Select all primary (master) nodes
    ReplicasOnly, // Select only replica nodes (for testing purposes)
    All,   // Select all nodes (both primary and replica)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutTarget {
    Local,
    Remote(ClusterNodeIdBuf),
}

impl Display for FanoutTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FanoutTarget::Local => write!(f, "FanoutTarget{{type: Local}}"),
            FanoutTarget::Remote(node_id) => {
                let str = &node_id[0..node_id.len() - 1];
                let val = String::from_utf8_lossy(str);
                write!(f, "FanoutTarget(type: Remote, node_id: {val})")
            }
        }
    }
}

impl FanoutTarget {
    pub fn local() -> Self {
        Self::Local
    }

    pub fn remote(node_id: ClusterNodeIdBuf) -> Self {
        Self::Remote(node_id)
    }

    pub fn from_node_id(node_id: *const c_char) -> Option<Self> {
        let buf = copy_node_id(node_id)?;
        Some(Self::Remote(buf))
    }

    pub fn is_local(&self) -> bool {
        matches!(self, FanoutTarget::Local)
    }

    pub fn address(&self) -> String {
        match self {
            FanoutTarget::Local => "Local".to_string(),
            FanoutTarget::Remote(node_id) => {
                let str = &node_id[0..node_id.len() - 1];
                String::from_utf8_lossy(str).to_string()
            }
        }
    }
}

fn copy_node_id(node_id: *const c_char) -> Option<ClusterNodeIdBuf> {
    if node_id.is_null() {
        return None;
    }
    let mut buf: ClusterNodeIdBuf = [0; VALKEYMODULE_NODE_ID_LEN as usize + 1];
    unsafe {
        ptr::copy_nonoverlapping(
            node_id as *const u8,
            buf.as_mut_ptr(),
            VALKEYMODULE_NODE_ID_LEN as usize,
        );
    }
    Some(buf)
}

/// Helper function to check if the Valkey server version is considered "legacy" (e.g., < 9).
/// In legacy versions, client read-only status might not be reliably determinable.
fn is_valkey_version_legacy(context: &Context) -> bool {
    context.get_server_version().map_or(false, |version| version.major < 9)
}

pub fn compute_query_fanout_mode(context: &Context) -> FanoutTargetMode {
    if FORCE_REPLICAS_READONLY.load(Ordering::Relaxed) {
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
                log::warn!("Could not determine client read-only status, defaulting to Random fanout mode.");
                FanoutTargetMode::Random
            }
        }
    }
}

/// Convenience method for FanoutSearchTarget with the default behavior
pub fn get_fanout_targets(ctx: &Context, target_mode: FanoutTargetMode) -> Vec<FanoutTarget> {
    get_fanout_targets_with_creators(
        ctx,
        FanoutTarget::local,
        |node_info| FanoutTarget::remote(node_info.node_buf),
        target_mode,
    )
}

/// Generic method for getting targets with custom target creators
pub(super) fn get_fanout_targets_with_creators<T, F1, F2>(
    ctx: &Context,
    create_local_target: F1,
    create_remote_target: F2,
    target_mode: FanoutTargetMode,
) -> Vec<T>
where
    F1: Fn() -> T,
    F2: Fn(&ClusterNodeInfo) -> T,
{
    match target_mode {
        FanoutTargetMode::Primary => {
            // Select all primary (master) nodes
            get_targets_filtered(
                ctx,
                create_local_target,
                create_remote_target,
                |node_info| node_info.is_primary(),
            )
        }
        FanoutTargetMode::ReplicasOnly => {
            // Select only replica nodes
            get_targets_filtered(
                ctx,
                create_local_target,
                create_remote_target,
                |node_info| !node_info.is_primary(),
            )
        }
        FanoutTargetMode::All => {
            // Select all nodes (both primary and replica)
            get_targets_filtered(ctx, create_local_target, create_remote_target, |_| true)
        }
        FanoutTargetMode::Random => {
            let mut selected_targets = Vec::new();
            let mut num_nodes: usize = 0;
            let nodes_ids = unsafe {
                ValkeyModule_GetClusterNodesList
                    .expect("ValkeyModule_GetClusterNodesList function is unavailable")(
                    ctx.ctx as *mut ValkeyModuleCtx,
                    &mut num_nodes,
                )
            };

            if num_nodes == 0 || nodes_ids.is_null() {
                return Default::default(); // No nodes available
            }

            let nodes = unsafe { std::slice::from_raw_parts(nodes_ids, num_nodes) };

            // todo: better capacity estimation
            let mut grouped_nodes: HashMap<String, Vec<T>> =
                HashMap::with_capacity(nodes.len() / 4);

            for &node_id in nodes {
                let Some(node_info) = get_cluster_node_info(ctx, node_id) else {
                    continue;
                };
                if node_info.is_failed() {
                    continue; // Skip failed nodes
                }
                // Group nodes by shard ID
                let shard_id = if node_info.is_primary() {
                    node_info.node_id().to_string()
                } else {
                    node_info.master_id().to_string()
                };
                let target = if node_info.is_local() {
                    create_local_target()
                } else {
                    create_remote_target(&node_info)
                };
                grouped_nodes.entry(shard_id).or_default().push(target);
            }

            unsafe {
                crate::ValkeyModule_FreeClusterNodesList
                    .expect("ValkeyModule_FreeClusterNodesList function does not exist")(
                    nodes_ids
                )
            };

            // Now select one random node per shard
            let mut rng = rng();
            for mut nodes in grouped_nodes.into_values() {
                let index = rng.random_range(0..=nodes.len() - 1);
                let target = nodes.swap_remove(index);
                selected_targets.push(target);
            }

            selected_targets
        }
    }
}

#[derive(Debug)]
pub struct ClusterNodeInfo {
    node_buf: ClusterNodeIdBuf,
    ip_buf: [u8; INET6_ADDR_STR_LEN],
    #[allow(unused_variables)]
    port: u32,
    master_buf: ClusterNodeIdBuf,
    flags: u32,
}

impl Hash for ClusterNodeInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.node_buf.hash(state);
    }
}

impl ClusterNodeInfo {
    pub fn is_primary(&self) -> bool {
        self.flags & REDISMODULE_NODE_MASTER != 0
    }

    pub fn is_local(&self) -> bool {
        self.flags & VALKEYMODULE_NODE_MYSELF != 0
    }

    pub fn node_id(&self) -> &str {
        unsafe {
            std::str::from_utf8_unchecked(&self.node_buf[..VALKEYMODULE_NODE_ID_LEN as usize])
        }
    }

    pub fn master_id(&self) -> &str {
        unsafe {
            std::str::from_utf8_unchecked(&self.master_buf[..VALKEYMODULE_NODE_ID_LEN as usize])
        }
    }

    pub fn addr(&self) -> Ipv6Addr {
            // Find the null terminator or use the full length
        let end = self.ip_buf
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(INET6_ADDR_STR_LEN);
        // Convert bytes to string slice
        let ip_str = std::str::from_utf8(&self.ip_buf[..end]).unwrap_or("::1");
        // Parse the string as an IPv6 address
        ip_str.parse::<Ipv6Addr>().unwrap_or(Ipv6Addr::LOCALHOST) 
    }

    pub fn is_failed(&self) -> bool {
        self.flags & (VALKEYMODULE_NODE_PFAIL | VALKEYMODULE_NODE_FAIL) != 0
    }
}

/// Fetches detailed information about a specific cluster node given its ID.
/// Returns `None` if the node information cannot be retrieved.
pub fn get_cluster_node_info(ctx: &Context, node_id: *const c_char) -> Option<ClusterNodeInfo> {
    let mut master_buf: ClusterNodeIdBuf = [0; MASTER_ID_LEN];
    let node_buf: ClusterNodeIdBuf = [0; (VALKEYMODULE_NODE_ID_LEN as usize) + 1];
    let mut ip_buf: [u8; INET6_ADDR_STR_LEN] = [0; INET6_ADDR_STR_LEN];
    let mut port: c_int = 0;
    let mut flags_: c_int = 0;

    let added = unsafe {
        ValkeyModule_GetClusterNodeInfo
            .expect("ValkeyModule_GetClusterNodeInfo function is unavailable")(
            ctx.ctx as *mut ValkeyModuleCtx,
            node_id,
            ip_buf.as_mut_ptr() as *mut c_char,
            master_buf.as_mut_ptr() as *mut c_char,
            &mut port,
            &mut flags_,
        ) == VALKEYMODULE_OK as c_int
    };

    if !added {
        log::debug!("Failed to get node info for node {node_id:?}, skipping node...");
        return None;
    }

    Some(ClusterNodeInfo {
        node_buf,
        ip_buf,
        port: port as u32,
        master_buf,
        flags: flags_ as u32,
    })
}

/// Retrieves and groups cluster nodes by their shard IDs.
/// Each shard ID maps to a vector of node addresses (as CStrings).
/// Primary nodes are grouped by their own node ID, while replica nodes are grouped by their master's node ID.
pub fn get_grouped_cluster_nodes(ctx: &Context) -> ClusterNodeMap {
    let mut num_nodes: usize = 0;
    let nodes_ids = unsafe {
        ValkeyModule_GetClusterNodesList
            .expect("ValkeyModule_GetClusterNodesList function is unavailable")(
            ctx.ctx as *mut ValkeyModuleCtx,
            &mut num_nodes,
        )
    };

    if num_nodes == 0 || nodes_ids.is_null() {
        return Default::default(); // No nodes available
    }

    let nodes = unsafe { std::slice::from_raw_parts(nodes_ids, num_nodes) };

    // todo: better capacity estimation
    let mut grouped_nodes: HashMap<String, Vec<ClusterNodeInfo>> = HashMap::with_capacity(nodes.len() / 4);

    for &node_id in nodes {
        let Some(node_info) = get_cluster_node_info(ctx, node_id) else {
            continue;
        };
        if node_info.is_failed() {
            continue; // Skip failed nodes
        }
        // Group nodes by shard ID
        let shard_id = if node_info.is_primary() {
            node_info.node_id().to_string()
        } else {
            node_info.master_id().to_string()
        };
        grouped_nodes.entry(shard_id).or_default().push(node_info);
    }

    unsafe {
        crate::ValkeyModule_FreeClusterNodesList
            .expect("ValkeyModule_FreeClusterNodesList function does not exist")(nodes_ids)
    };

    grouped_nodes
}

fn get_targets_filtered<T, F1, F2>(
    ctx: &Context,
    create_local_target: F1,
    create_remote_target: F2,
    filter: impl Fn(&ClusterNodeInfo) -> bool,
) -> Vec<T>
where
    F1: Fn() -> T,
    F2: Fn(&ClusterNodeInfo) -> T,
{
    let mut selected_targets = Vec::new();

    let mut num_nodes: usize = 0;
    let nodes_ids = unsafe {
        ValkeyModule_GetClusterNodesList
            .expect("ValkeyModule_GetClusterNodesList function is unavailable")(
            ctx.ctx as *mut ValkeyModuleCtx,
            &mut num_nodes,
        )
    };

    if num_nodes == 0 || nodes_ids.is_null() {
        return selected_targets; // No nodes available
    }

    let nodes = unsafe { std::slice::from_raw_parts(nodes_ids, num_nodes) };

    for &node_id in nodes {
        if let Some(node_info) = get_cluster_node_info(ctx, node_id) {
            if !node_info.is_failed() && filter(&node_info) {
                if node_info.is_local() {
                    selected_targets.push(create_local_target());
                } else {
                    selected_targets.push(create_remote_target(&node_info));
                }
            }
        }
    }

    unsafe {
        crate::ValkeyModule_FreeClusterNodesList
            .expect("ValkeyModule_FreeClusterNodesList function does not exist")(nodes_ids)
    };

    selected_targets
}
