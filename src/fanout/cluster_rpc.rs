use super::cluster_message::{serialize_request_message, RequestMessage};
use super::fanout_error::{FanoutError, NO_CLUSTER_NODES_AVAILABLE};
use super::fanout_targets::{FanoutTarget};
use super::{is_clustered, is_multi_or_lua};
use core::time::Duration;
use std::collections::HashMap;
use std::os::raw::{c_char, c_uchar, c_int};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use crate::{Context, DetachedContext, Status, ValkeyModuleCtx, VALKEYMODULE_OK};
use crate::{RedisModuleCtx, ValkeyError, ValkeyResult};

use super::utils::generate_id;

pub(super) const CLUSTER_REQUEST_MESSAGE: u8 = 0x01;
pub(super) const CLUSTER_RESPONSE_MESSAGE: u8 = 0x02;
pub(super) const CLUSTER_ERROR_MESSAGE: u8 = 0x03;

// todo: make these configurable?
pub static DEFAULT_CLUSTER_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) type ResponseCallback = Arc<dyn Fn(Result<&[u8], FanoutError>, FanoutTarget) + Send + Sync>;
pub(crate) type RequestHandlerCallback =
    Arc<dyn Fn(&Context, &[u8], &mut Vec<u8>, FanoutTarget) -> ValkeyResult<()> + Send + Sync>;

struct InFlightRequest {
    db: i32,
    request_handler: RequestHandlerCallback,
    response_handler: ResponseCallback,
    outstanding: AtomicU64,
    timer_id: u64,
    timed_out: AtomicBool,
}

impl InFlightRequest {
    fn is_timed_out(&self) -> bool {
        self.timed_out.load(Ordering::Relaxed)
    }

    fn time_out(&self) {
        self.timed_out.store(true, Ordering::Relaxed);
    }

    fn raise_error(&self, error: FanoutError, target: FanoutTarget) {
        if !self.is_timed_out() {
            (self.response_handler)(Err(error), target);
        }
    }

    fn rpc_done(&self) {
        let remaining = self.outstanding.fetch_sub(1, Ordering::AcqRel);
        if remaining == 1 {
            // Last response received, clean up
            let mut map = INFLIGHT_REQUESTS.lock().unwrap();
            map.remove(&self.timer_id);
        }
    }
}

type InFlightRequestMap = HashMap<u64, InFlightRequest>;

static INFLIGHT_REQUESTS: LazyLock<Mutex<InFlightRequestMap>> = LazyLock::new(|| Mutex::new(InFlightRequestMap::default()));

fn get_inflight_requests_map() -> std::sync::MutexGuard<'static, InFlightRequestMap> {
    INFLIGHT_REQUESTS.lock().expect("InFlightRequests lock poisoned")
}

fn on_command_timeout(ctx: &Context, id: u64) {
    let map = get_inflight_requests_map();
    if let Some(request) = map.get(&id) {
        let _ = ctx.stop_timer::<u64>(request.timer_id);
        request.time_out();
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
    targets: &[FanoutTarget],
    request_handler: RequestHandlerCallback,
    response_handler: ResponseCallback,
    timeout: Option<Duration>,
) -> ValkeyResult<()> {
    validate_cluster_exec(ctx)?;

    let id = generate_id();
    let db = get_current_db(ctx);

    let mut buf = Vec::with_capacity(512);
    serialize_request_message(&mut buf, id, db, request_buf);

    let mut node_count = 0;
    for node in targets.iter() {
        if let FanoutTarget::Remote(node_id) = node {
            let target_id = node_id.as_ptr().cast::<c_char>();
            let status =
                send_cluster_message(ctx, target_id, CLUSTER_REQUEST_MESSAGE, buf.as_slice());
            if status == Status::Err {
                let msg = format!("Failed to send message to node {}", node.address());
                ctx.log_warning(&msg);
                continue;
            }
            node_count += 1;
        }
    }

    if node_count == 0 {
        return Err(ValkeyError::Str(NO_CLUSTER_NODES_AVAILABLE));
    }

    let timeout = timeout.unwrap_or_else(get_cluster_command_timeout);
    let timer_id = ctx.create_timer(timeout, on_command_timeout, id);

    let request = InFlightRequest {
        db,
        request_handler,
        response_handler,
        timer_id,
        outstanding: AtomicU64::new(node_count as u64),
        timed_out: AtomicBool::new(false),
    };

    let mut map = get_inflight_requests_map();
    map.insert(id, request);
    Ok(())
}

// Send the response back to the original sender
fn send_message_internal(
    ctx: &Context,
    msg_type: u8,
    request_id: u64,
    sender_id: *const c_char,
    buf: &[u8],
) -> Status {
    let mut dest = Vec::with_capacity(1024);
    let db = get_current_db(ctx);
    serialize_request_message(&mut dest, request_id, db, buf);
    send_cluster_message(ctx, sender_id, msg_type, buf)
}

// Send the response back to the original sender
fn send_response_message(
    ctx: &Context,
    request_id: u64,
    sender_id: *const c_char,
    buf: &[u8],
) -> Status {
    send_message_internal(ctx, CLUSTER_RESPONSE_MESSAGE, request_id, sender_id, buf)
}

fn send_error_response(
    ctx: &Context,
    request_id: u64,
    target_node: *const c_char,
    error: FanoutError,
) -> Status {
    let mut buf: Vec<u8> = Vec::with_capacity(512);
    error.serialize(&mut buf);
    send_message_internal(ctx, CLUSTER_ERROR_MESSAGE, request_id, target_node, &buf)
}

fn parse_cluster_message(
    ctx: &Context,
    sender_id: Option<*const c_char>,
    payload: *const c_uchar,
    len: u32,
) -> Option<RequestMessage> {
    let buffer = unsafe { std::slice::from_raw_parts(payload, len as usize) };
    match RequestMessage::new(buffer) {
        Ok(msg) => {
            let buf = msg.buf;
            if buf.is_empty() {
                let request_id = msg.request_id;
                let msg = format!("BUG: empty response payload for request ({request_id})");
                ctx.log_warning(&msg);
                if let Some(sender_id) = sender_id {
                    let error = FanoutError::failed(msg.clone()); // todo: better error
                    let _ = send_error_response(ctx, 0, sender_id, error);
                }
                return None;
            }
            Some(msg)
        }
        Err(e) => {
            let msg = format!("Failed to parse cluster message: {e}");
            ctx.log_warning(&msg);
            if let Some(sender_id) = sender_id {
                let error = FanoutError::failed(msg.clone()); // todo: better error
                let _ = send_error_response(ctx, 0, sender_id, error);
            }
            None
        }
    }
}

/// Processes a valid request by executing the command and sending back the response.
fn process_request<'a>(ctx: &'a Context, message: RequestMessage<'a>, sender_id: *const c_char) {
    let request_id = message.request_id;

    let map = get_inflight_requests_map();
    let Some(inflight_request) = map.get(&request_id) else {
        // The inflight request should always be found.
        // But if the timer expires, the inflight request will be removed from the map.
        let ctx = DetachedContext::new();
        let msg = format!("BUG: Inflight request not found for request_id: {request_id}",);
        ctx.log_warning(&msg);
        return;
    };
    let handler = inflight_request.request_handler.clone();
    drop(map);

    let save_db = get_current_db(ctx);
    set_current_db(ctx, message.db);

    let mut dest = Vec::with_capacity(1024);
    let buf = message.buf;

    let target = FanoutTarget::from_node_id(sender_id).expect("Invalid target ID");
    let res = handler(ctx, buf, &mut dest, target);
    if let Err(e) = res {
        set_current_db(ctx, save_db);
        let msg = e.to_string();
        send_error_response(ctx, request_id, sender_id, e.into());
        ctx.log_warning(&msg);
        return;
    };

    set_current_db(ctx, save_db);
    if send_response_message(ctx, request_id, sender_id, &dest) == Status::Err {
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
        if crate::ValkeyModule_SendClusterMessage
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

fn get_current_db(ctx: &Context) -> i32 {
    unsafe { crate::RedisModule_GetSelectedDb.unwrap()(ctx.ctx) }
}

fn set_current_db(ctx: &Context, db: i32) -> Status {
    // Safety: RedisModule_SelectDb is safe to call. It is a bug in the valkey_module
    // if the function is not available.
    unsafe {
        match crate::RedisModule_SelectDb.unwrap()(ctx.ctx, db) {
            0 => Status::Ok,
            _ => Status::Err,
        }
    }
}

extern "C" fn on_request_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);
    let Some(message) = parse_cluster_message(&ctx, Some(sender_id), payload, len) else {
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

    let Some(message) = parse_cluster_message(&ctx, None, payload, len) else {
        return;
    };

    let request_id = message.request_id;

    // fetch corresponding inflight request by request_id
    let map = get_inflight_requests_map();
    let Some(request) = map.get(&request_id) else {
        // possible timeout. todo: pass timeout error?
        ctx.log_warning(&format!(
            "Failed to find inflight request for id {request_id}"
        ));
        return;
    };
    let handler = request.response_handler.clone();
    request.rpc_done();
    drop(map); // drop the lock before calling the handler

    let target = FanoutTarget::from_node_id(sender_id).expect("Invalid target ID");
    
    set_current_db(&ctx, message.db);
    handler(Ok(message.buf), target);
}

extern "C" fn on_error_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);

    let Some(message) = parse_cluster_message(&ctx, None, payload, len) else {
        return;
    };

    let request_id = message.request_id;

    // fetch corresponding inflight request by request_id
    let map = get_inflight_requests_map();
    let Some(request) = map.get(&request_id) else {
        // possible timeout. todo: pass timeout error?
        ctx.log_warning(&format!(
            "Failed to find inflight request for id {request_id}"
        ));
        return;
    };
    let handler = request.response_handler.clone();
    request.rpc_done();
    drop(map); // drop the lock before calling the handler

    let target = FanoutTarget::from_node_id(sender_id).expect("Invalid target ID");
    match FanoutError::deserialize(message.buf) {
        Ok((error, _)) => handler(Err(error), target),
        Err(_) => ctx.log_warning("Failed to deserialize error response"), // todo: pass error to handler?
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
    receiver_func: crate::ValkeyModuleClusterMessageReceiver,
) {
    unsafe {
        crate::ValkeyModule_RegisterClusterMessageReceiver
            .expect("ValkeyModule_RegisterClusterMessageReceiver is not available")(
            ctx.ctx as *mut ValkeyModuleCtx,
            type_,
            receiver_func,
        );
    }
}

/// Registers the cluster message handlers for request, response, and error messages.
/// This function should be called during module initialization
pub fn register_cluster_message_handlers(ctx: &Context) {
    // Register the cluster message handlers
    register_message_receiver(ctx, CLUSTER_REQUEST_MESSAGE, Some(on_request_received));
    register_message_receiver(ctx, CLUSTER_RESPONSE_MESSAGE, Some(on_response_received));
    register_message_receiver(ctx, CLUSTER_ERROR_MESSAGE, Some(on_error_received));
}
