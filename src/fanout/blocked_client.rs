use crate::fanout::FanoutOperation;
use std::ffi::c_void;
use std::os::raw::c_int;
use crate::{
    Context, ValkeyModule_BlockClient, ValkeyModule_BlockedClientMeasureTimeEnd,
    ValkeyModule_BlockedClientMeasureTimeStart, ValkeyModule_GetBlockedClientPrivateData,
    ValkeyModule_UnblockClient, ValkeyModuleCtx, ValkeyModuleString, raw,
};

const NO_TIMEOUT: i64 = 86400000;

#[repr(C)]
pub(super) struct BlockedClientPrivateData<OP>
where
    OP: FanoutOperation,
{
    operation: OP,
    timed_out: bool,
    error_count: usize,
}

impl<OP> BlockedClientPrivateData<OP>
where
    OP: FanoutOperation,
{
    pub(super) fn new(operation: OP, timed_out: bool, error_count: usize) -> Self {
        Self {
            operation,
            timed_out,
            error_count,
        }
    }
    fn reply(&mut self, ctx: &Context) {
        if self.timed_out {
            self.operation.generate_timeout_reply(ctx);
        } else if self.error_count > 0 {
            self.operation.generate_error_reply(ctx);
        } else {
            self.operation.generate_reply(ctx);
        }
    }
}

/// High-level wrapper for a blocked client.
pub(super) struct FanoutBlockedClient<T: FanoutOperation> {
    inner: *mut raw::ValkeyModuleBlockedClient,
    data: Option<Box<BlockedClientPrivateData<T>>>,
    time_measurement_ongoing: bool,
}

// We need to be able to send the inner pointer to another thread
unsafe impl<T: FanoutOperation> Send for FanoutBlockedClient<T> {}

impl<T> FanoutBlockedClient<T>
where
    T: FanoutOperation,
{
    pub fn new(ctx: &Context) -> Self {
        let bc_ptr = unsafe {
            ValkeyModule_BlockClient.unwrap()(
                ctx.ctx as *mut ValkeyModuleCtx,
                Some(reply_callback::<T>),
                None,
                // NOTE: We do not need a free callback because we handle freeing the data in the
                // reply callback. if FanoutOperation is made to not require non 'static, we need to
                // provide a free callback here to avoid memory leaks.
                None, // Some(free_callback::<T>),
                NO_TIMEOUT,
            )
        };
        Self {
            inner: bc_ptr,
            time_measurement_ongoing: false,
            data: None,
        }
    }

    /// Set the private data that will be passed back on unblock.
    pub fn set_reply_private_data(&mut self, private_data: BlockedClientPrivateData<T>) {
        self.data = Some(Box::new(private_data));
    }

    pub fn unblock(&mut self) {
        // Ensure any ongoing measurement is ended.
        self.measure_time_end();

        // Take private_data for local use.
        let private_data_ptr = self.data.take().map_or(std::ptr::null_mut(), |boxed| {
            Box::into_raw(boxed) as *mut c_void
        });

        // Call out to the C API to actually unblock.
        unsafe {
            ValkeyModule_UnblockClient.unwrap()(self.inner, private_data_ptr);
        }
    }

    /// Start measuring time for a blocked client.
    pub fn measure_time_start(&mut self) {
        if self.time_measurement_ongoing {
            return;
        }
        unsafe { ValkeyModule_BlockedClientMeasureTimeStart.unwrap()(self.inner) };
        self.time_measurement_ongoing = true;
    }

    /// End measuring time for a blocked client.
    pub fn measure_time_end(&mut self) {
        if !self.time_measurement_ongoing {
            return;
        }
        unsafe { ValkeyModule_BlockedClientMeasureTimeEnd.unwrap()(self.inner) };
        self.time_measurement_ongoing = false;
    }
}

impl<T: FanoutOperation> Drop for FanoutBlockedClient<T> {
    fn drop(&mut self) {
        // Ensure we try to unblock when the wrapper is dropped, following RAII.
        // swallow any panics to avoid unwinding across FFI boundaries.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.unblock();
        }))
        .ok();
    }
}

fn take_data<T>(data: *mut c_void) -> T {
    // Cast the *mut c_void supplied by the Valkey API to a raw pointer of our custom type.
    let data = data.cast::<T>();

    // Take back ownership of the original boxed data, so we can unbox it safely.
    // If we don't do this, the data's memory will be leaked.
    let data = unsafe { Box::from_raw(data) };

    *data
}

extern "C" fn reply_callback<T: FanoutOperation>(
    ctx: *mut ValkeyModuleCtx,
    _argv: *mut *mut ValkeyModuleString,
    _argc: c_int,
) -> c_int {
    let op_ptr = unsafe { ValkeyModule_GetBlockedClientPrivateData.unwrap()(ctx) };
    let ctx = Context::new(ctx as *mut raw::RedisModuleCtx);
    if op_ptr.is_null() {
        ctx.reply_error_string("No reply data");
    } else {
        // Cast to the correct type and then dereference once to get &mut ResponseContext<T>
        let mut response_ctx: BlockedClientPrivateData<T> = take_data(op_ptr);
        response_ctx.reply(&ctx);
    }
    0
}

extern "C" fn free_callback<T: FanoutOperation>(
    _ctx: *mut ValkeyModuleCtx,
    private_data: *mut c_void,
) {
    if !private_data.is_null() {
        unsafe {
            let boxed: Box<BlockedClientPrivateData<T>> =
                Box::from_raw(private_data as *mut BlockedClientPrivateData<T>);
            drop(boxed);
        }
    }
}
