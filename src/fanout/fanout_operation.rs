use super::cluster_rpc::{get_cluster_command_timeout, send_cluster_request};
use super::fanout_error::{FanoutError, FanoutResult};
use super::fanout_targets::{get_cluster_targets, FanoutTarget, FanoutTargetMode, compute_query_fanout_mode};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use crate::fanout::Serializable;
use crate::{
    BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyValue,
};
use super::utils::current_time_millis;


/// A trait representing a fan-out operation that can be performed across cluster nodes.
/// It handles processing node-specific requests, managing responses, and generating the
/// final reply to the client.
///
/// # Type Parameters
/// - `Request`: The type representing the request to be sent to the target nodes.
/// - `Response`: The type representing the response expected from the target nodes.
///
pub trait FanoutOperation<Request, Response>: Send {
    /// Return the name of the fanout operation.
    fn name() -> &'static str;

    /// Handle a local request on the current node, returning the response or an error.
    fn get_local_response(ctx: &Context, req: Request) -> ValkeyResult<Response>;

    /// Return the timeout duration for the entire fanout operation.
    /// This timeout applies to the overall operation, not individual RPC calls.
    fn get_timeout(&self) -> Duration {
        get_cluster_command_timeout()
    }

    /// Generate the request to be sent to each target node.
    fn generate_request(&mut self) -> Request;

    /// Called once per successful response from a target node.
    fn on_response(&mut self, resp: Response, target: FanoutTarget);

    fn on_error(&mut self, error: FanoutError, target: FanoutTarget) {
        // Log the error with context
        log::error!(
            "Fanout operation {}, failed for target {}: {}",
            Self::name(),
            target,
            error,
        );
    }

    /// Called once all responses have been received, or an error has occurred.
    /// This is where the final reply to the client should be generated.
    /// If there were any errors, the default implementation will generate an error reply.
    fn generate_reply(&mut self, ctx: &Context);
}

/// A trait for invoking fanout operations across cluster nodes.
/// It manages the sending of requests, handling responses, and coordinating the overall operation.
/// # Type Parameters
/// - `H`: The type implementing the `FanoutOperation` trait.
/// - `Request`: The type representing the request to be sent to the target nodes.
/// - `Response`: The type representing the response expected from the target nodes.
///
pub trait FanoutInvoker<H, Request, Response>: Send
where
    H: FanoutOperation<Request, Response>,
    Request: Send,
    Response: Send,
{
    /// Start the fanout operation by generating the request and invoking RPCs to cluster nodes.
    fn start(&mut self, ctx: &Context, handler: H) -> ValkeyResult<()>;

    /// Invoke a remote RPC call to all target nodes with the provided request and callback.
    /// The callback will be called once per target node with the status and response once the
    /// per-node call completes.
    fn invoke_remote_rpc(
        &mut self,
        context: &Context,
        req: Request,
        callback: Box<dyn Fn(FanoutResult<Response>, FanoutTarget) + Send + Sync>,
        timeout: Duration,
    ) -> ValkeyResult<()>;
}

/// Internal structure to manage the state of an ongoing fanout operation.
/// It tracks outstanding RPCs, errors, and coordinates the final reply generation.
struct FanoutState<H, Request, Response>
where
    H: FanoutOperation<Request, Response>,
{
    handler: H,
    outstanding: usize,
    deadline: i64,
    thread_ctx: ThreadSafeContext<BlockedClient>,
    errors: Vec<FanoutError>,
    __phantom: std::marker::PhantomData<(Request, Response)>,
}

impl<H, Request, Response> FanoutState<H, Request, Response>
where
    H: FanoutOperation<Request, Response>,
    Request: Send,
    Response: Send,
{
    pub fn new(context: &Context, handler: H) -> Self {
        let blocked_client = context.block_client();
        Self {
            handler,
            deadline: 0,
            outstanding: 0,
            thread_ctx: ThreadSafeContext::with_blocked_client(blocked_client),
            errors: Vec::new(),
            __phantom: Default::default(),
        }
    }

    fn generate_request(&mut self) -> Request {
        self.handler.generate_request()
    }

    fn rpc_done(&mut self) -> bool {
        if self.outstanding > 0 {
            self.outstanding = self.outstanding.saturating_sub(1);
            if self.outstanding == 0 {
                self.on_completion();
                return true;
            }
        } else {
            // Equivalent to C++ CHECK(outstanding_ > 0);
            // panic!("Outstanding RPCs is already zero in rpc_done");
        }
        false
    }

    fn on_error(&mut self, error: FanoutError, _target: FanoutTarget) {
        self.errors.push(error);
    }

    fn generate_reply(&mut self, ctx: &Context) {
        if self.errors.is_empty() {
            self.handler.generate_reply(ctx);
        } else {
            self.generate_error_reply(ctx);
        }
    }

    fn handle_rpc_callback(&mut self, resp: FanoutResult<Response>, target: FanoutTarget) -> bool {
        match resp {
            Ok(response) => {
                // Handle successful response
                self.handler.on_response(response, target);
            }
            Err(err) => {
                self.on_error(err, target);
            }
        }
        self.rpc_done()
    }

    fn handle_local_request(&mut self, ctx: &Context, request: Request) {
        let target = FanoutTarget::Local;
        let resp = match H::get_local_response(ctx, request) {
            Ok(response) => Ok(response),
            Err(err) => Err(err.into()),
        };
        self.handle_rpc_callback(resp, target);
    }

    fn on_completion(&mut self) {
        // No errors, generate a successful reply
        let thread_ctx = &self.thread_ctx;
        let ctx = thread_ctx.lock(); // ????? do we need to lock to reply?
        self.generate_reply(&ctx);
    }

    fn is_operation_timed_out(&self) -> bool {
        current_time_millis() >= self.deadline
    }

    fn generate_error_reply(&self, ctx: &Context) {
        let internal_error_log_prefix: String =
            format!("Failure(fanout) in operation {}: Internal error on node with address ", H::name());

        let mut error_message = String::new();

        if !self.errors.is_empty() {
            error_message = "Internal error found.".to_string();
            for err in &self.errors {
                ctx.log_warning(&format!("{internal_error_log_prefix}{err:?}"));
            }
        }

        if error_message.is_empty() {
            error_message = "Unknown error".to_string();
        }

        // Reply to a client with an error
        ctx.reply_error_string(&error_message);
    }
}

pub struct BaseFanoutInvoker<H, Request, Response>
where
    H: FanoutOperation<Request, Response>,
    Request: Send,
    Response: Send,
{
    targets: Vec<FanoutTarget>,
    __phantom: std::marker::PhantomData<(H, Request, Response)>,
}

impl<H, Request, Response> BaseFanoutInvoker<H, Request, Response>
where
    H: FanoutOperation<Request, Response>,
    Request: Send,
    Response: Send,
{
    pub fn new() -> Self {
        Self {
            targets: Vec::new(),
            __phantom: Default::default(),
        }
    }
}

impl<H, Request, Response> FanoutInvoker<H, Request, Response> for BaseFanoutInvoker<H, Request, Response>
where
    H: FanoutOperation<Request, Response> + 'static,
    Request: Serializable + Send + 'static,
    Response: Serializable + Send + 'static,
{
    fn start(&mut self, ctx: &Context, handler: H) -> ValkeyResult<()> {
        let mut handler = handler;
        let timeout = handler.get_timeout();
        let deadline = current_time_millis() + timeout.as_millis() as i64;

        let req = handler.generate_request();
        let mut state = FanoutState::new(ctx, handler);
        state.deadline = deadline;

        let target_mode = compute_query_fanout_mode(ctx);
        if target_mode == FanoutTargetMode::Local {
            state.outstanding = 1;
            state.handle_local_request(ctx, req);
            return Ok(());
        }

        self.targets = get_cluster_targets(ctx, target_mode);
        let outstanding = self.targets.len();

        let has_local = self.targets.iter().any(|t| t.is_local());

        state.outstanding = outstanding;

        if has_local {
            if outstanding > 1 {
                let req_local = state.generate_request();
                state.handle_local_request(ctx, req_local);
            } else {
                state.handle_local_request(ctx, req);
                return Ok(());
            }
        }

        let state_mutex = Mutex::new(state);
        self.invoke_remote_rpc(
            ctx,
            req,
            Box::new(move |res, target| {
                let mut state = state_mutex.lock().expect("mutex poisoned");
                state.handle_rpc_callback(res, target);
            }),
            timeout,
        )
    }

    fn invoke_remote_rpc(
        &mut self,
        ctx: &Context,
        req: Request,
        callback: Box<dyn Fn(FanoutResult<Response>, FanoutTarget) + Send + Sync>,
        timeout: Duration,
    ) -> ValkeyResult<()> {
        let request_handler = Arc::new(
            |ctx: &Context,
             req_buf: &[u8],
             dest: &mut Vec<u8>,
             _target: FanoutTarget|
             -> ValkeyResult<()> {
                match Request::deserialize(req_buf) {
                    Ok(request) => {
                        let response = H::get_local_response(ctx, request)?;
                        response
                            .serialize(dest);
                    }
                    Err(_e) => {
                        // todo: change this
                        let msg = _e.to_string();
                        return Err(ValkeyError::String(msg));
                    }
                }
                Ok(())
            },
        );

        let response_handler = Arc::new(
            move |res: Result<&[u8], FanoutError>, target: FanoutTarget| match res {
                Ok(buf) => match Response::deserialize(buf) {
                    Ok(resp) => callback(Ok(resp), target),
                    Err(_e) => {
                        let err = FanoutError::serialization(Default::default());
                        callback(Err(err), target);
                    }
                },
                Err(err) => callback(Err(err), target),
            },
        );

        let mut buf = Vec::with_capacity(512); // 512 to select preset
        req.serialize(&mut buf);

        send_cluster_request(
            ctx,
            &buf,
            &self.targets,
            request_handler,
            response_handler,
            Some(timeout),
        )
    }
}

pub fn exec_fanout_request<H, Request, Response>(
    ctx: &Context,
    operation: H,
) -> ValkeyResult<ValkeyValue>
where
    H: FanoutOperation<Request, Response> + 'static,
    Request: Serializable + Send + 'static,
    Response: Serializable + Send + 'static,
{
    let mut invoker = BaseFanoutInvoker::new();
    invoker.start(ctx, operation)?;

    // We will reply later, from the callbacks
    Ok(ValkeyValue::NoReply)
}

/// Perform a remote request
pub fn perform_remote_request<RequestT, ResponseT, TrackerT, F, CallbackFn>(
    request: RequestT,
    address: &str,
    tracker: Arc<TrackerT>,
    rpc_invoker: F,
    callback_logic: CallbackFn,
    timeout_ms: Option<i32>,
) where
    RequestT: Send + 'static,
    ResponseT: Send + 'static,
    TrackerT: Send + Sync + 'static,
    F: FnOnce(RequestT, Box<dyn FnOnce(Result<ResponseT, String>) + Send>, Option<i32>)
        + Send
        + 'static,
    CallbackFn: FnOnce(Result<ResponseT, String>, Arc<TrackerT>, String) + Send + 'static,
{
    let address_owned = address.to_string();

    let callback = Box::new(move |result: Result<ResponseT, String>| {
        callback_logic(result, tracker, address_owned);
    });

    rpc_invoker(request, callback, timeout_ms);
}
