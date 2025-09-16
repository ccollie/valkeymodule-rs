use crate::{ValkeyError, ValkeyResult};
use super::encoding::{try_read_byte_slice, write_byte_slice};

/// Fanout error. Designed mostly for compactness since it's sent over the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutError {
    pub kind: ErrorKind,

    /// Extra context about error
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[repr(u8)]
pub enum ErrorKind {
    /// Something went wrong
    Failed = 0,

    /// A cluster node was unreachable.
    NodeUnreachable = 1,

    /// One or more of the nodes in the cluster did not respond to the request in time.
    Timeout = 2,

    /// Unknown message type.
    UnknownMessageType = 3,

    Permissions = 4,

    /// The user does not have access to one or more keys required to fulfill the request.
    KeyPermissions = 5,

    /// Error during serialization or deserialization of the request or response.
    Serialization = 6,

    BadRequestId = 7,
    
    Internal = 8,
}

pub const FAILED_ERROR: &str = "Failed";
pub const PERMISSIONS_ERROR: &str = "Permission denied";
pub const KEY_PERMISSIONS_ERROR: &str = "User does not have access to one or more keys";
pub const UNKNOWN_MESSAGE_TYPE_ERROR: &str = "Unknown message type.";
pub const SERIALIZATION_ERROR: &str = "Serialization error";
pub const BAD_REQUEST_ID_ERROR: &str = "Bad request id";
pub const TIMEOUT_ERROR: &str = "Fanout command timed out";
pub const NODE_UNREACHABLE_ERROR: &str = "Cluster node unreachable";
pub const NO_CLUSTER_NODES_AVAILABLE: &str = "No cluster nodes available";
pub const INTERNAL_ERROR: &str = "Internal error";

impl ErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Failed => FAILED_ERROR,
            Self::Permissions => PERMISSIONS_ERROR,
            Self::KeyPermissions => KEY_PERMISSIONS_ERROR,
            Self::UnknownMessageType => UNKNOWN_MESSAGE_TYPE_ERROR,
            Self::Serialization => SERIALIZATION_ERROR,
            Self::BadRequestId => BAD_REQUEST_ID_ERROR,
            Self::Timeout => TIMEOUT_ERROR,
            Self::NodeUnreachable => NODE_UNREACHABLE_ERROR,
            Self::Internal => INTERNAL_ERROR,
        }
    }
}

pub type FanoutResult<T> = Result<T, FanoutError>;

impl FanoutError {
    pub fn failed<S: Into<String>>(description: S) -> Self {
        Self {
            message: description.into(),
            kind: ErrorKind::Failed,
        }
    }

    pub fn serialization<S: Into<String>>(description: S) -> Self {
        Self {
            message: description.into(),
            kind: ErrorKind::Serialization,
        }
    }

    pub fn key_permissions(description: String) -> Self {
        Self {
            message: description,
            kind: ErrorKind::KeyPermissions,
        }
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(self.kind as u8);
        write_byte_slice(buf, self.message.as_str().as_ref());
    }

    pub fn deserialize(buf: &[u8]) -> ValkeyResult<(Self, &[u8])> {
        if buf.len() < 2 {
            return Err(ValkeyError::Str("Buffer too small to contain Error"));
        }

        let kind = ErrorKind::try_from(buf[0])?;
        let mut buf = &buf[1..];

        let extra = try_read_byte_slice(&mut buf)
            .map_err(|_| ValkeyError::Str("Failed to read Error extra"))?;

        let extra = if !extra.is_empty() {
            String::from_utf8(extra.to_vec())
                .map_err(|_| ValkeyError::Str("Invalid UTF-8 in Error extra"))?
        } else {
            String::new()
        };

        Ok((
            Self {
                kind,
                message: extra,
            },
            buf,
        ))
    }
}

impl TryFrom<u8> for ErrorKind {
    type Error = ValkeyError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ErrorKind::Failed),
            1 => Ok(ErrorKind::NodeUnreachable),
            2 => Ok(ErrorKind::Timeout),
            3 => Ok(ErrorKind::UnknownMessageType),
            4 => Ok(ErrorKind::Permissions),
            5 => Ok(ErrorKind::KeyPermissions),
            6 => Ok(ErrorKind::Serialization),
            7 => Ok(ErrorKind::BadRequestId),
            8 => Ok(ErrorKind::Internal),
            _ => {
                let msg = format!("Invalid error kind: {value}");
                Err(ValkeyError::String(msg))
            }
        }
    }
}

impl core::fmt::Display for ErrorKind {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        write!(fmt, "{}", self.as_str())
    }
}

impl core::fmt::Display for FanoutError {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        if self.message.is_empty() {
            write!(fmt, "{}", self.kind.as_str())
        } else {
            write!(fmt, "{}", self.message)
        }
    }
}

impl core::error::Error for FanoutError {
    fn description(&self) -> &str {
        if self.message.is_empty() {
            return self.kind.as_str();
        }
        &self.message
    }
    fn cause(&self) -> Option<&dyn (core::error::Error)> {
        None
    }
}

/// Converts an error string into a structured Error object.
/// Maps known error strings to specific ErrorKind variants,
/// or falls back to a general Failed error with the original message.
fn convert_from_string(err: &str) -> FanoutError {
    if err.is_empty() {
        return FanoutError {
            kind: ErrorKind::Failed,
            message: String::new(),
        };
    }


    match err {
        KEY_PERMISSIONS_ERROR => FanoutError {
            kind: ErrorKind::KeyPermissions,
            message: String::new(),
        },
        PERMISSIONS_ERROR => FanoutError {
            kind: ErrorKind::Permissions,
            message: String::new(),
        },
        FAILED_ERROR => FanoutError::failed(String::new()),
        INTERNAL_ERROR => FanoutError {
            kind: ErrorKind::Internal,
            message: String::new(),
        },
        NODE_UNREACHABLE_ERROR => FanoutError {
            kind: ErrorKind::NodeUnreachable,
            message: String::new(),
        },
        UNKNOWN_MESSAGE_TYPE_ERROR => FanoutError {
            kind: ErrorKind::UnknownMessageType,
            message: String::new(),
        },
        SERIALIZATION_ERROR => FanoutError::serialization(String::new()),
        BAD_REQUEST_ID_ERROR => FanoutError {
            kind: ErrorKind::BadRequestId,
            message: String::new(),
        },
        TIMEOUT_ERROR => FanoutError {
            kind: ErrorKind::Timeout,
            message: String::new(),
        },
        _ => FanoutError::failed(err.to_string()),
    }
}

impl From<&str> for FanoutError {
    fn from(value: &str) -> Self {
        convert_from_string(value)
    }
}

impl From<String> for FanoutError {
    fn from(value: String) -> Self {
        convert_from_string(&value)
    }
}

impl From<ValkeyError> for FanoutError {
    fn from(value: ValkeyError) -> Self {
        match value {
            ValkeyError::Str(msg) => convert_from_string(msg),
            ValkeyError::String(err) => convert_from_string(&err),
            _ => FanoutError::failed(value.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error_consts;
    use valkey_module::ValkeyError;

    #[test]
    fn test_fanout_error_constructors() {
        let error = FanoutError::failed("test failure".to_string());
        assert_eq!(error.kind, ErrorKind::Failed);
        assert_eq!(error.message, "test failure");

        let error = FanoutError::serialization("serialization issue".to_string());
        assert_eq!(error.kind, ErrorKind::Serialization);
        assert_eq!(error.message, "serialization issue");

        let error = FanoutError::key_permissions("key access denied".to_string());
        assert_eq!(error.kind, ErrorKind::KeyPermissions);
        assert_eq!(error.message, "key access denied");
    }

    #[test]
    fn test_fanout_error_display() {
        // Error with a message
        let error = FanoutError::failed("custom message".to_string());
        assert_eq!(format!("{error}"), "custom message");

        // Error without a message
        let error = FanoutError {
            kind: ErrorKind::Failed,
            message: String::new(),
        };
        assert_eq!(format!("{error}"), "Failed");
    }

    #[test]
    fn test_fanout_error_error_trait() {
        let error = FanoutError::failed("test error".to_string());
        assert_eq!(error.to_string(), "test error");

        // Test with an empty message
        let error = FanoutError {
            kind: ErrorKind::Timeout,
            message: String::new(),
        };
        assert_eq!(error.to_string(), "Fanout command timed out");
    }

    #[test]
    fn test_serialize_deserialize_with_message() {
        let original = FanoutError::failed("test error message".to_string());
        let mut buf = Vec::new();
        original.serialize(&mut buf);

        let (deserialized, remaining) = FanoutError::deserialize(&buf).unwrap();
        assert_eq!(original, deserialized);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_serialize_deserialize_empty_message() {
        let original = FanoutError {
            kind: ErrorKind::Timeout,
            message: String::new(),
        };
        let mut buf = Vec::new();
        original.serialize(&mut buf);

        let (deserialized, remaining) = FanoutError::deserialize(&buf).unwrap();
        assert_eq!(original, deserialized);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_serialize_deserialize_all_error_kinds() {
        let error_kinds = [
            ErrorKind::Failed,
            ErrorKind::NodeUnreachable,
            ErrorKind::Timeout,
            ErrorKind::UnknownMessageType,
            ErrorKind::Permissions,
            ErrorKind::KeyPermissions,
            ErrorKind::Serialization,
            ErrorKind::BadRequestId,
            ErrorKind::Internal,
        ];

        for kind in &error_kinds {
            let original = FanoutError {
                kind: *kind,
                message: format!("test message for {kind:?}"),
            };
            let mut buf = Vec::new();
            original.serialize(&mut buf);

            let (deserialized, remaining) = FanoutError::deserialize(&buf).unwrap();
            assert_eq!(original, deserialized);
            assert!(remaining.is_empty());
        }
    }

    #[test]
    fn test_deserialize_with_remaining_data() {
        let original = FanoutError::failed("test".to_string());
        let mut buf = Vec::new();
        original.serialize(&mut buf);
        buf.extend_from_slice(b"extra data");

        let (deserialized, remaining) = FanoutError::deserialize(&buf).unwrap();
        assert_eq!(original, deserialized);
        assert_eq!(remaining, b"extra data");
    }

    #[test]
    fn test_deserialize_errors() {
        // Buffer too small
        let result = FanoutError::deserialize(&[]);
        assert!(result.is_err());

        let result = FanoutError::deserialize(&[0]);
        assert!(result.is_err());

        // Invalid error kind
        let mut buf = vec![255]; // Invalid error kind
        buf.push(0); // Empty message length
        let result = FanoutError::deserialize(&buf);
        assert!(result.is_err());

        // Invalid UTF-8 in a message
        let mut buf = vec![0]; // Valid error kind (Failed)
        buf.push(3); // Message length = 3
        buf.extend_from_slice(&[0xFF, 0xFE, 0xFD]); // Invalid UTF-8
        let result = FanoutError::deserialize(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_buffer_too_small_for_message() {
        let mut buf = vec![0]; // Valid error kind (Failed)
        buf.push(10); // Message length = 10, but we won't provide 10 bytes
        buf.extend_from_slice(b"short"); // Only 5 bytes

        let result = FanoutError::deserialize(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_str() {
        // Test known error constants
        let error = FanoutError::from(error_consts::COMMAND_DESERIALIZATION_ERROR);
        assert_eq!(error.kind, ErrorKind::Serialization);
        assert_eq!(error.message, "");

        let error = FanoutError::from(error_consts::NO_CLUSTER_NODES_AVAILABLE);
        assert_eq!(error.kind, ErrorKind::NodeUnreachable);
        assert_eq!(error.message, "");

        let error = FanoutError::from(error_consts::KEY_READ_PERMISSION_ERROR);
        assert_eq!(error.kind, ErrorKind::KeyPermissions);
        assert_eq!(error.message, "");

        // Test generic permission error
        let error = FanoutError::from("permission denied for user");
        assert_eq!(error.kind, ErrorKind::Permissions);
        assert_eq!(error.message, "");

        // Test unknown error
        let error = FanoutError::from("unknown error message");
        assert_eq!(error.kind, ErrorKind::Failed);
        assert_eq!(error.message, "unknown error message");

        // Test empty string
        let error = FanoutError::from("");
        assert_eq!(error.kind, ErrorKind::Failed);
        assert_eq!(error.message, "");
    }

    #[test]
    fn test_from_valkey_error() {
        let valkey_error = ValkeyError::Str("test error");
        let fanout_error = FanoutError::from(valkey_error);
        assert_eq!(fanout_error.kind, ErrorKind::Failed);
        assert_eq!(fanout_error.message, "test error");

        let valkey_error = ValkeyError::String("another test".to_string());
        let fanout_error = FanoutError::from(valkey_error);
        assert_eq!(fanout_error.kind, ErrorKind::Failed);
        assert_eq!(fanout_error.message, "another test");

        // Test with a known error constant
        let valkey_error = ValkeyError::Str(error_consts::COMMAND_DESERIALIZATION_ERROR);
        let fanout_error: FanoutError = valkey_error.into();
        assert_eq!(fanout_error.kind, ErrorKind::Serialization);
        assert_eq!(fanout_error.message, "");
    }

    #[test]
    fn test_convert_from_string_edge_cases() {
        // Test specific error constants
        assert_eq!(convert_from_string(FAILED_ERROR).kind, ErrorKind::Failed);
        assert_eq!(
            convert_from_string(PERMISSIONS_ERROR).kind,
            ErrorKind::Permissions
        );
        assert_eq!(
            convert_from_string(UNKNOWN_MESSAGE_TYPE_ERROR).kind,
            ErrorKind::UnknownMessageType
        );
        assert_eq!(
            convert_from_string(SERIALIZATION_ERROR).kind,
            ErrorKind::Serialization
        );
        assert_eq!(
            convert_from_string(BAD_REQUEST_ID_ERROR).kind,
            ErrorKind::BadRequestId
        );
        assert_eq!(convert_from_string(TIMEOUT_ERROR).kind, ErrorKind::Timeout);
        assert_eq!(
            convert_from_string(NODE_UNREACHABLE_ERROR).kind,
            ErrorKind::NodeUnreachable
        );
        assert_eq!(
            convert_from_string(INTERNAL_ERROR).kind,
            ErrorKind::Internal
        );
    }

    #[test]
    fn test_fanout_result_type_alias() {
        let success: FanoutResult<i32> = Ok(42);
        assert_eq!(success, Ok(42));

        let failure: FanoutResult<i32> = Err(FanoutError::failed("test".to_string()));
        assert!(failure.is_err());
    }

    #[test]
    fn test_error_kind_repr_u8() {
        // Test that the repr(u8) values match what we expect
        assert_eq!(ErrorKind::Failed as u8, 0);
        assert_eq!(ErrorKind::NodeUnreachable as u8, 1);
        assert_eq!(ErrorKind::Timeout as u8, 2);
        assert_eq!(ErrorKind::UnknownMessageType as u8, 3);
        assert_eq!(ErrorKind::Permissions as u8, 4);
        assert_eq!(ErrorKind::KeyPermissions as u8, 5);
        assert_eq!(ErrorKind::Serialization as u8, 6);
        assert_eq!(ErrorKind::BadRequestId as u8, 7);
        assert_eq!(ErrorKind::Internal as u8, 8);
    }
}
