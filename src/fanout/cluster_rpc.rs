use super::fanout_error::{FanoutError, NO_CLUSTER_NODES_AVAILABLE};
use super::fanout_message::{serialize_request_message, FanoutMessage};
use super::utils::{is_clustered, is_multi_or_lua};
use crate::fanout::cluster_map::{NodeId, CURRENT_NODE_ID};
use crate::fanout::registry::get_fanout_request_handler;
use crate::fanout::{
    get_current_db, set_current_db, FanoutResponseCallback, FanoutResult, NodeInfo,
};
use crate::{
    Context, RedisModuleCtx, RedisModule_GetSelectedDb, RedisModule_SelectDb, Status, ValkeyError,
    ValkeyModuleClusterMessageReceiver, ValkeyModuleCtx,
    ValkeyModule_RegisterClusterMessageReceiver, ValkeyModule_SendClusterMessage, ValkeyResult,
    VALKEYMODULE_OK,
};
use core::time::Duration;
use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::os::raw::{c_char, c_int, c_uchar};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

const FANOUT_REQUEST_MESSAGE: u8 = 0x01;
const FANOUT_RESPONSE_MESSAGE: u8 = 0x02;
const FANOUT_ERROR_MESSAGE: u8 = 0x03;

pub static DEFAULT_CLUSTER_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

struct InFlightRequest {
    id: u64,
    targets: Arc<Vec<NodeInfo>>,
    response_handler: FanoutResponseCallback,
    outstanding: AtomicU64,
    timer_id: u64,
    timed_out: AtomicBool,
}

impl InFlightRequest {
    fn rpc_done(&self, ctx: &Context, guard: &mut MutexGuard<InFlightRequestMap>) {
        if self.outstanding.fetch_sub(1, Ordering::Relaxed) == 1 {
            // Last response received, clean up
            self.cancel_timer(ctx);
            guard.remove(&self.id);
        }
    }

    fn cancel_timer(&self, ctx: &Context) {
        let _ = ctx.stop_timer::<u64>(self.timer_id);
    }

    fn get_target_node(&self, sender_id: *const c_char) -> &NodeInfo {
        // SAFETY: sender_id is expected to be a valid pointer to a 40-byte node ID
        let sender = NodeId::from_raw(sender_id);

        // Binary search to find the NodeInfo associated with the target
        self.targets
            .binary_search_by(|node| node.id.cmp(&sender))
            .ok()
            .and_then(|idx| self.targets.get(idx))
            .expect("cluster rpc: target node lookup failed")
    }

    fn handle_response(&self, _ctx: &Context, resp: FanoutResult<&[u8]>, sender_id: *const c_char) {
        let target_node = self.get_target_node(sender_id);

        (self.response_handler)(resp, target_node);
    }
}

type InFlightRequestMap = HashMap<u64, InFlightRequest>;

static INFLIGHT_REQUESTS: LazyLock<Mutex<InFlightRequestMap>> = LazyLock::new(Mutex::default);

static REQUEST_ID: AtomicU64 = AtomicU64::new(0);

/// Generate a unique request ID
///
/// This id only needs to be unique per node, so a simple atomic counter is enough.
///
/// The node that initiates a request is always the one waiting for responses, so there's no ambiguity
/// about which node "owns" a particular request ID.
///
/// There's no need for global uniqueness because:
///
///     - Each node only looks up requests in its own INFLIGHT_REQUESTS map
///     - Request IDs never need to be coordinated across nodes
///     - Two different nodes can safely use the same ID simultaneously for different requests
///
fn generate_id() -> u64 {
    let mut id = REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    if id == 0 {
        // could equally have been random
        let curr_id = *CURRENT_NODE_ID;
        let hasher = RandomState::new();
        id = hasher.hash_one(curr_id.as_bytes());
        REQUEST_ID.store(id, Ordering::SeqCst);
    }
    id
}

fn on_request_timeout(ctx: &Context, id: u64) {
    // We only mark the request as timed out the first time through. The actual removal from the map
    // happens when the last response arrives or when we hit the timeout again.

    let mut map = INFLIGHT_REQUESTS.lock().unwrap();
    if let Some(request) = map.get(&id) {
        if request.timed_out.load(Ordering::Relaxed) {
            let _ = ctx.stop_timer::<u64>(request.timer_id);
            // Already timed out, remove from the map
            map.remove(&id);
            return;
        }
        request.timed_out.store(true, Ordering::Relaxed);

        let local_node_id = CURRENT_NODE_ID.raw_ptr();

        request.handle_response(ctx, Err(FanoutError::timeout()), local_node_id);
    }
}

fn validate_cluster_exec(ctx: &Context) -> ValkeyResult<()> {
    if !is_clustered(ctx) {
        return Err(ValkeyError::Str("Cluster mode is not enabled"));
    }
    if is_multi_or_lua(ctx) {
        return Err(ValkeyError::Str("Cannot execute in MULTI or Lua context"));
    }
    Ok(())
}

pub fn get_cluster_command_timeout() -> Duration {
    DEFAULT_CLUSTER_REQUEST_TIMEOUT
}

pub(super) fn send_cluster_request(
    ctx: &Context,
    request_buf: &[u8],
    targets: Arc<Vec<NodeInfo>>,
    handler: &str,
    response_handler: FanoutResponseCallback,
    timeout: Option<Duration>,
) -> ValkeyResult<()> {
    validate_cluster_exec(ctx)?;

    let id = generate_id();
    let db = get_current_db(ctx);

    let mut buf = Vec::with_capacity(512);
    serialize_request_message(&mut buf, id, db, handler, request_buf);

    let mut node_count = 0;

    for node in targets.iter().filter(|&node| !node.is_local()) {
        let target_id = node.id.raw_ptr();
        let status = send_cluster_message(ctx, target_id, FANOUT_REQUEST_MESSAGE, buf.as_slice());
        if status == Status::Err {
            let msg = format!("Failed to send message to node {}", node.address());
            ctx.log_warning(&msg);
            continue;
        }
        node_count += 1;
    }

    if node_count == 0 {
        return Err(ValkeyError::Str(NO_CLUSTER_NODES_AVAILABLE));
    }

    let timeout = timeout.unwrap_or_else(get_cluster_command_timeout);
    let timer_id = ctx.create_timer(timeout, on_request_timeout, id);

    let request = InFlightRequest {
        id,
        response_handler,
        timer_id,
        outstanding: AtomicU64::new(node_count as u64),
        timed_out: AtomicBool::new(false),
        targets: targets.clone(),
    };

    let mut map = INFLIGHT_REQUESTS
        .lock()
        .expect("Failed to lock INFLIGHT_REQUESTS mutex");
    map.insert(id, request);
    Ok(())
}

/// Send the response back to the original sender
fn send_message_internal(
    ctx: &Context,
    msg_type: u8,
    request_id: u64,
    sender_id: *const c_char,
    handler: &str,
    buf: &[u8],
) -> Status {
    let mut dest = Vec::with_capacity(1024);
    let db = get_current_db(ctx);
    serialize_request_message(&mut dest, request_id, db, handler, buf);
    send_cluster_message(ctx, sender_id, msg_type, &dest)
}

/// Send the response back to the original sender
fn send_response_message(
    ctx: &Context,
    request_id: u64,
    sender_id: *const c_char,
    handler: &str,
    buf: &[u8],
) -> Status {
    send_message_internal(
        ctx,
        FANOUT_RESPONSE_MESSAGE,
        request_id,
        sender_id,
        handler,
        buf,
    )
}

fn send_error_response(
    ctx: &Context,
    request_id: u64,
    target_node: *const c_char,
    error: FanoutError,
) -> Status {
    let mut buf: Vec<u8> = Vec::with_capacity(512);
    error.serialize(&mut buf);
    send_message_internal(ctx, FANOUT_ERROR_MESSAGE, request_id, target_node, "", &buf)
}

fn parse_fanout_message(
    ctx: &'_ Context,
    sender_id: *const c_char,
    payload: *const c_uchar,
    len: u32,
) -> Option<FanoutMessage<'_>> {
    // SAFETY: payload is expected to be a valid pointer to a byte array of length `len` coming
    // from Valkey
    let buffer = unsafe { std::slice::from_raw_parts(payload, len as usize) };
    match FanoutMessage::new(buffer) {
        Ok(msg) => Some(msg),
        Err(err) => {
            let node = NodeId::from_raw(sender_id);
            let msg = format!("Failed to parse fanout message from node {node}: {err}");
            ctx.log_warning(&msg);
            None
        }
    }
}

/// Processes a valid request by executing the command and sending back the response.
fn process_request<'a>(ctx: &'a Context, message: FanoutMessage<'a>, sender_id: *const c_char) {
    let request_id = message.request_id;

    let Some(handler) = get_fanout_request_handler(&message.handler) else {
        let e = FanoutError::invalid_message();
        send_error_response(ctx, request_id, sender_id, e);
        let msg = format!(
            "No handler registered for fanout operation '{}'",
            message.handler
        );
        ctx.log_warning(&msg);
        return;
    };

    let save_db = get_current_db(ctx);
    set_current_db(ctx, message.db);

    let mut dest = Vec::with_capacity(1024);
    let buf = message.buf;
    
    // Execute the handler and capture the result
    let res = handler(ctx, buf, &mut dest);

    set_current_db(ctx, save_db);
    if let Err(e) = res {
        let msg = e.to_string();
        send_error_response(ctx, request_id, sender_id, e);
        ctx.log_warning(&msg);
        return;
    };

    if send_response_message(ctx, request_id, sender_id, &message.handler, &dest) == Status::Err {
        let msg = format!("Failed to send response message to node {sender_id:?}");
        ctx.log_warning(&msg);
    }
}

/// Sends a message to a specific cluster node.
///
/// # Arguments
///
/// * `target_node_id` - The 40-byte hex ID of the target node.
/// * `msg_type` - The type of the message to send.
/// * `message_body` - The raw byte payload of the message.
///
/// # Returns
///
/// `Status::Ok` on success, `Status::Err` with a message on failure.
pub fn send_cluster_message(
    ctx: &Context,
    target_node_id: *const c_char,
    msg_type: u8,
    message_body: &[u8],
) -> Status {
    unsafe {
        if ValkeyModule_SendClusterMessage
            .expect("ValkeyModule_SendClusterMessage is not available")(
            ctx.ctx as *mut ValkeyModuleCtx,
            target_node_id,
            msg_type,
            message_body.as_ptr().cast::<c_char>(),
            message_body.len() as u32,
        ) == VALKEYMODULE_OK as c_int
        {
            Status::Ok
        } else {
            Status::Err
        }
    }
}

/// Handles incoming requests from requester nodes in the cluster.
extern "C" fn on_request_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);
    let Some(message) = parse_fanout_message(&ctx, sender_id, payload, len) else {
        return;
    };
    process_request(&ctx, message, sender_id);
}

/// Handles responses from other nodes in the cluster. The receiver is the original sender of
/// the request.
extern "C" fn on_response_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);

    let Some(message) = parse_fanout_message(&ctx, sender_id, payload, len) else {
        return;
    };

    let request_id = message.request_id;

    // fetch corresponding inflight request by request_id
    let mut map = INFLIGHT_REQUESTS.lock().unwrap();
    let Some(request) = map.get(&request_id) else {
        ctx.log_warning(&format!(
            "Failed to find inflight request for id {request_id}. Possible timeout.",
        ));
        return;
    };

    request.rpc_done(&ctx, &mut map);
    request.handle_response(&ctx, Ok(message.buf), sender_id);
}

extern "C" fn on_error_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);

    let local_node_id = CURRENT_NODE_ID.raw_ptr();
    let Some(message) = parse_fanout_message(&ctx, local_node_id, payload, len) else {
        return;
    };

    let request_id = message.request_id;

    // fetch corresponding inflight request by request_id
    let map = INFLIGHT_REQUESTS.pin();
    let Some(request) = map.get(&request_id) else {
        ctx.log_warning(&format!(
            "Failed to find inflight request for id {request_id}. Possible timeout.",
        ));
        return;
    };

    set_current_db(&ctx, message.db);
    request.rpc_done(&ctx, &mut map);

    match FanoutError::deserialize(message.buf) {
        Ok((error, _)) => request.handle_response(&ctx, Err(error), sender_id),
        Err(_) => {
            ctx.log_warning("Failed to deserialize error response");
            let err = FanoutError::invalid_message();
            request.handle_response(&ctx, Err(err), sender_id);
        }
    }
}

// Safety: RedisModule_GetSelectedDb is safe to call
fn get_current_db(ctx: &Context) -> i32 {
    unsafe { RedisModule_GetSelectedDb.unwrap()(ctx.ctx) }
}

fn set_current_db(ctx: &Context, db: i32) -> Status {
    // Safety: RedisModule_SelectDb is safe to call. It is a bug in the valkey_module
    // if the function is not available.
    unsafe {
        match RedisModule_SelectDb.unwrap()(ctx.ctx, db) {
            0 => Status::Ok,
            _ => Status::Err,
        }
    }
}

/// Registers a callback function to handle incoming cluster messages.
/// This should typically be called during module initialization (e.g., ValkeyModule_OnLoad).
///
/// ## Arguments
///
/// * `receiver_func` - The function pointer matching the expected signature
///   `ValkeyModuleClusterMessageReceiverFunc`.
///
/// ## Safety
///
/// The provided `receiver_func` must be valid for the lifetime of the module
/// and correctly handle the arguments passed by Valkey.
fn register_message_receiver(
    ctx: &Context, // Static method as it's usually done once at load time
    type_: u8,
    receiver_func: ValkeyModuleClusterMessageReceiver,
) {
    unsafe {
        ValkeyModule_RegisterClusterMessageReceiver
            .expect("ValkeyModule_RegisterClusterMessageReceiver is not available")(
            ctx.ctx as *mut ValkeyModuleCtx,
            type_,
            receiver_func,
        );
    }
}

/// Registers the cluster message handlers for request, response, and error messages.
/// This function should be called during module initialization
pub(super) fn register_cluster_message_handlers(ctx: &Context) {
    // Register the cluster message handlers
    register_message_receiver(ctx, FANOUT_REQUEST_MESSAGE, Some(on_request_received));
    register_message_receiver(ctx, FANOUT_RESPONSE_MESSAGE, Some(on_response_received));
    register_message_receiver(ctx, FANOUT_ERROR_MESSAGE, Some(on_error_received));
}
