use std::collections::HashMap;
use super::fanout_operation::FanoutOperation;
use super::serialization::{Deserialized, Serializable, Serialized};
use crate::fanout::{FanoutError, FanoutResult};
use std::sync::{Arc, LazyLock, Mutex};
use crate::{Context, ValkeyError, ValkeyResult};

/// Type-erased function pointer for executing a fanout operation.
/// This allows us to store different fanout operations with different
/// Request/Response types in the same registry.
pub(super) type RequestHandlerCallback =
    Arc<dyn Fn(&Context, &[u8], &mut Vec<u8>) -> FanoutResult + Send + Sync>;

/// A registry for fanout operations that allows type-erased storage and retrieval
/// of [`FanoutOperation`] implementations.
struct FanoutOperationRegistry {
    operations: Mutex<HashMap<&'static str, RequestHandlerCallback>>,
}

impl FanoutOperationRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            operations: Mutex::default(),
        }
    }

    /// Register a fanout operation by name.
    ///
    /// # Type Parameters
    /// - `OP`: The operation type implementing FanoutOperation
    pub fn register<OP>(&self) -> ValkeyResult<()>
    where
        OP: FanoutOperation,
        OP::Request: Serializable,
        OP::Response: Serializable,
    {
        let name = OP::name();

        let request_handler = Arc::new(
            |ctx: &Context, req_buf: &[u8], dest: &mut Vec<u8>| -> FanoutResult {
                match OP::Request::deserialize(req_buf) {
                    Ok(request) => {
                        let response = OP::get_local_response(ctx, request)?;
                        response.serialize(dest);
                    }
                    Err(e) => {
                        let msg =
                            format!("Failed to deserialize {} fanout request: {e}", OP::name());
                        return Err(FanoutError::serialization(&msg));
                    }
                }
                Ok(())
            },
        );

        let mut map = self.operations.lock()?;
        if map.contains_key(name) {
            return Err(ValkeyError::String(format!(
                "Operation '{name}' is already registered"
            )));
        }

        map.insert(name, request_handler);

        Ok(())
    }

    /// Execute a registered fanout operation by name.
    ///
    /// # Arguments
    /// - `ctx`: The Valkey context
    /// - `name`: The name of the operation to execute
    /// - `payload`: Serialized request data
    /// - `dest`: destination buffer. This will be sent back to requester
    fn execute(
        &self,
        ctx: &Context,
        name: &str,
        payload: &[u8],
        dest: &mut Vec<u8>,
    ) -> FanoutResult<()> {
        let executor = self.get_operation_by_name(name, true).unwrap();
        executor(ctx, payload, dest)
    }

    fn get_operation_by_name(
        &self,
        name: &str,
        must_exist: bool,
    ) -> Option<RequestHandlerCallback> {
        let map = self.operations.lock().unwrap();
        match map.get(name) {
            Some(op) => Some(op.clone()),
            None => {
                if must_exist {
                    panic!("Fanout Operation '{name}' not found in registry");
                } else {
                    None
                }
            }
        }
    }

    /// Check if an operation is registered.
    pub fn contains(&self, name: &str) -> bool {
        let map = self.operations.lock().unwrap();
        map.contains_key(name)
    }

    /// Get the list of all registered operation names.
    pub fn list_operations(&self) -> Vec<&'static str> {
        let mut ops = Vec::new();
        let map = self.operations.lock().unwrap();
        for key in map.keys() {
            ops.push(*key);
        }
        ops.sort();
        ops
    }
}

static FANOUT_REGISTRY: LazyLock<FanoutOperationRegistry> =
    LazyLock::new(FanoutOperationRegistry::new);

/// Register a fanout operation.
///
/// # Type Parameters
/// - `OP`: The operation type implementing FanoutOperation
pub fn register_fanout_operation<OP>() -> ValkeyResult<()>
where
    OP: FanoutOperation,
    OP::Request: Serializable,
    OP::Response: Serializable,
{
    FANOUT_REGISTRY.register::<OP>()
}

/// Execute a registered fanout operation by name.
///
/// # Arguments
/// - `ctx`: The Valkey context
/// - `name`: The name of the operation to execute
/// - `payload`: Serialized request data
/// - `dest`: destination buffer. This will be sent back to requester
pub fn handle_fanout_request(
    ctx: &Context,
    name: &str,
    payload: &[u8],
    dest: &mut Vec<u8>,
) -> FanoutResult {
    FANOUT_REGISTRY.execute(ctx, name, payload, dest)
}

pub(super) fn get_fanout_request_handler(name: &str) -> Option<RequestHandlerCallback> {
    FANOUT_REGISTRY.get_operation_by_name(name, false)
}

pub fn get_registered_fanout_operations() -> Vec<&'static str> {
    FANOUT_REGISTRY.list_operations()
}