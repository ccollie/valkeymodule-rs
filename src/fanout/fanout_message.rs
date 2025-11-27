use super::fanout_error::INVALID_MESSAGE_ERROR;
use super::encoding::{
    try_read_signed_varint, try_read_string, try_read_uvarint, write_byte_slice,
    write_signed_varint, write_uvarint,
};
use crate::fanout::{FanoutError, FanoutResult};

pub const FANOUT_MESSAGE_VERSION: u16 = 1;
pub const FANOUT_MESSAGE_MARKER: u32 = 0xBADCAB;

/// Header for messages exchanged between cluster nodes.
#[derive(Debug, Clone)]
pub(super) struct FanoutMessageHeader {
    pub version: u16,
    /// Unique ID for this request, used to match responses.
    pub request_id: u64,
    /// The database to use for this request.
    pub db: i32,
    /// The name of the handler to process this message.
    pub handler: String,
    /// Reserved for future use (e.g., for larger payloads, we may compress the data)
    pub reserved: u16,
}

impl FanoutMessageHeader {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        // Start with the marker
        write_marker(buf);

        // Encode the version as 2 bytes (u16)
        write_uvarint(buf, self.version as u64);

        // Encode request_id as uvarint
        write_uvarint(buf, self.request_id);

        // Encode db as signed varint
        write_signed_varint(buf, self.db as i64);

        // Encode handler as a string
        write_byte_slice(buf, self.handler.as_bytes());

        write_uvarint(buf, self.reserved as u64);
    }

    /// Deserializes a MessageHeader from the beginning of the buffer.
    /// Returns the deserialized header and the payload buffer.
    pub fn deserialize(buf: &[u8]) -> FanoutResult<(Self, &[u8])> {
        // Read and validate the marker
        let mut buf = skip_marker(buf)?;

        let version = read_uvarint(&mut buf)
            .map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))?
            as u16;

        // Decode request_id as uvarint
        let request_id = read_uvarint(&mut buf)?;

        let db = try_read_signed_varint(&mut buf)
            .map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))?
            as i32;

        // Read handler as a string
        let handler = try_read_string(&mut buf)
            .map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))?;

        // Read msg_type and reserved as direct bytes
        let reserved = read_uvarint(&mut buf)? as u16;

        Ok((
            FanoutMessageHeader {
                version,
                request_id,
                handler,
                db,
                reserved,
            },
            buf,
        ))
    }
}

fn read_uvarint(input: &mut &[u8]) -> FanoutResult<u64> {
    try_read_uvarint(input).map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))
}

fn write_marker(slice: &mut Vec<u8>) {
    let bytes = FANOUT_MESSAGE_MARKER.to_le_bytes();
    slice.extend_from_slice(bytes.as_ref());
}

fn skip_marker(input: &[u8]) -> FanoutResult<&[u8]> {
    let size = size_of_val(&FANOUT_MESSAGE_MARKER);
    if input.len() < size {
        return Err(FanoutError::serialization(INVALID_MESSAGE_ERROR));
    }
    let (int_bytes, rest) = input.split_at(size);
    let marker = u32::from_le_bytes(
        int_bytes
            .try_into()
            .map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))?,
    );
    if marker != FANOUT_MESSAGE_MARKER {
        return Err(FanoutError::serialization(INVALID_MESSAGE_ERROR));
    }

    Ok(rest)
}

/// Represents a decoded fanout message with its header and payload.
pub(super) struct FanoutMessage<'a> {
    pub buf: &'a [u8],
    /// Unique ID for this request, used to match responses.
    pub request_id: u64,
    /// The name of the handler to process this message.
    pub handler: String,
    /// The database to use for this request.
    pub db: i32,
}

impl<'a> FanoutMessage<'a> {
    /// Creates a new FanoutMessage by parsing the provided buffer.
    /// Returns an error if the buffer is invalid or cannot be parsed.
    ///
    /// # Arguments
    /// - `buf`: The byte slice containing the serialized request message.
    ///
    /// # Returns
    /// A Result containing the FanoutMessage or a FanoutError.
    ///
    /// ## Note
    /// An empty payload is possible because of protobuf encoding of default values.
    /// A prost struct with all fields at their default values (e.g., all numeric fields are 0,
    /// booleans are false, strings are "", etc.) serializes into an empty byte buffer.
    /// There is no data to write because everything is implicit.
    pub fn new(buf: &'a [u8]) -> FanoutResult<Self> {
        let (header, buf) = FanoutMessageHeader::deserialize(buf)?;
        let FanoutMessageHeader {
            request_id,
            db,
            handler,
            ..
        } = header;

        Ok(Self {
            buf,
            request_id,
            handler,
            db,
        })
    }
}

pub(super) fn serialize_request_message(
    dest: &mut Vec<u8>,
    request_id: u64,
    db: i32,
    handler: &str,
    serialized_request: &[u8],
) {
    let header = FanoutMessageHeader {
        version: FANOUT_MESSAGE_VERSION,
        request_id,
        db,
        handler: handler.to_string(),
        reserved: 0,
    };
    header.serialize(dest);
    dest.extend_from_slice(serialized_request);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fanout::ErrorKind;

    #[test]
    fn test_cluster_message_header_serialize_deserialize_basic() {
        let header = FanoutMessageHeader {
            version: FANOUT_MESSAGE_VERSION,
            request_id: 12345,
            db: 0,
            handler: "test_handler".to_string(),
            reserved: 0,
        };

        let mut buf = Vec::new();
        header.serialize(&mut buf);

        let (deserialized_header, remaining_buf) = FanoutMessageHeader::deserialize(&buf).unwrap();

        assert_eq!(deserialized_header.version, FANOUT_MESSAGE_VERSION);
        assert_eq!(deserialized_header.request_id, 12345);
        assert_eq!(deserialized_header.db, 0);
        assert_eq!(deserialized_header.reserved, 0);
        assert_eq!(deserialized_header.handler, "test_handler");
        assert_eq!(remaining_buf.len(), 0);
    }

    #[test]
    fn test_cluster_message_header_serialize_deserialize_with_negative_db() {
        let header = FanoutMessageHeader {
            version: FANOUT_MESSAGE_VERSION,
            request_id: u64::MAX,
            db: -15,
            handler: "negative_db_handler".to_string(),
            reserved: 42,
        };

        let mut buf = Vec::new();
        header.serialize(&mut buf);

        let (deserialized_header, remaining_buf) = FanoutMessageHeader::deserialize(&buf).unwrap();

        assert_eq!(deserialized_header.version, FANOUT_MESSAGE_VERSION);
        assert_eq!(deserialized_header.request_id, u64::MAX);
        assert_eq!(deserialized_header.db, -15);
        assert_eq!(deserialized_header.reserved, 42);
        assert_eq!(deserialized_header.handler, "negative_db_handler");
        assert_eq!(remaining_buf.len(), 0);
    }

    #[test]
    fn test_cluster_message_header_deserialize_with_remaining_data() {
        let header = FanoutMessageHeader {
            version: FANOUT_MESSAGE_VERSION,
            request_id: 999,
            db: 5,
            handler: "handler_with_extra".to_string(),
            reserved: 1,
        };

        let mut buf = Vec::new();
        header.serialize(&mut buf);
        buf.extend_from_slice(b"extra_data");

        let (deserialized_header, remaining_buf) = FanoutMessageHeader::deserialize(&buf).unwrap();

        assert_eq!(deserialized_header.version, FANOUT_MESSAGE_VERSION);
        assert_eq!(deserialized_header.request_id, 999);
        assert_eq!(deserialized_header.db, 5);
        assert_eq!(deserialized_header.reserved, 1);
        assert_eq!(deserialized_header.handler, "handler_with_extra");
        assert_eq!(remaining_buf, b"extra_data");
    }

    #[test]
    fn test_cluster_message_header_deserialize_empty_buffer() {
        let buf = &[];
        let result = FanoutMessageHeader::deserialize(buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_cluster_message_header_deserialize_buffer_too_small() {
        let buf = &[0x01, 0x02, 0x03]; // Too small for a complete header
        let result = FanoutMessageHeader::deserialize(buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_cluster_message_header_deserialize_invalid_marker() {
        let mut buf = Vec::new();

        // Write invalid marker
        let invalid_marker = 0xDEADBEEF_u32;
        buf.extend_from_slice(&invalid_marker.to_le_bytes());

        // Add some dummy data for version, request_id, db, reserved
        buf.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05]);

        let result = FanoutMessageHeader::deserialize(&buf);
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_cluster_message_header_deserialize_incomplete_version() {
        let mut buf = Vec::new();

        // Write correct marker but incomplete data
        buf.extend_from_slice(&FANOUT_MESSAGE_MARKER.to_le_bytes());
        // No version data

        let result = FanoutMessageHeader::deserialize(&buf);
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_cluster_message_header_deserialize_incomplete_request_id() {
        let mut buf = Vec::new();

        // Write the correct marker and version
        buf.extend_from_slice(&FANOUT_MESSAGE_MARKER.to_le_bytes());
        buf.push(1); // version as varint
        // Missing request_id and other fields

        let result = FanoutMessageHeader::deserialize(&buf);
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    fn assert_serialization_error(result: FanoutResult<(FanoutMessageHeader, &[u8])>) {
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.kind, ErrorKind::Serialization);
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_cluster_message_header_deserialize_incomplete_db() {
        let mut buf = Vec::new();

        // Write the correct marker, version, and request_id
        buf.extend_from_slice(&FANOUT_MESSAGE_MARKER.to_le_bytes());
        buf.push(1); // version as varint
        buf.push(42); // request_id as varint
        // Missing db and reserved fields

        let result = FanoutMessageHeader::deserialize(&buf);
        assert_serialization_error(result);
    }

    #[test]
    fn test_cluster_message_header_deserialize_incomplete_reserved() {
        let mut buf = Vec::new();

        // Write the correct marker, version, request_id, and db
        buf.extend_from_slice(&FANOUT_MESSAGE_MARKER.to_le_bytes());
        buf.push(1); // version as varint
        buf.push(42); // request_id as varint
        buf.push(0); // db as signed varint (0 encodes as 0)
        // Missing reserved field

        let result = FanoutMessageHeader::deserialize(&buf);
        assert_serialization_error(result);
    }

    #[test]
    fn test_write_marker() {
        let mut buf = Vec::new();
        write_marker(&mut buf);

        assert_eq!(buf.len(), 4);
        let marker = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(marker, FANOUT_MESSAGE_MARKER);
    }

    #[test]
    fn test_skip_marker_valid() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&FANOUT_MESSAGE_MARKER.to_le_bytes());
        buf.extend_from_slice(b"remaining_data");

        let result = skip_marker(&buf);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"remaining_data");
    }

    #[test]
    fn test_skip_marker_invalid() {
        let mut buf = Vec::new();
        let invalid_marker = 0xDEADBEEF_u32;
        buf.extend_from_slice(&invalid_marker.to_le_bytes());
        buf.extend_from_slice(b"remaining_data");

        let result = skip_marker(&buf);
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.kind, ErrorKind::Serialization);
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_skip_marker_buffer_too_small() {
        let buf = &[0x01, 0x02]; // Only 2 bytes, need 4 for u32 marker

        let result = skip_marker(buf);
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.kind, ErrorKind::Serialization);
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_skip_marker_empty_buffer() {
        let buf = &[];

        let result = skip_marker(buf);
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.kind, ErrorKind::Serialization);
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_serialize_request_message() {
        let mut buf = Vec::new();
        let request_data = b"test_request_data";

        serialize_request_message(&mut buf, 123, 5, "handler", request_data);

        // Verify we can deserialize the header
        let (header, remaining) = FanoutMessageHeader::deserialize(&buf).unwrap();

        assert_eq!(header.version, FANOUT_MESSAGE_VERSION);
        assert_eq!(header.request_id, 123);
        assert_eq!(header.db, 5);
        assert_eq!(header.reserved, 0);
        assert_eq!(header.handler, "handler");
        assert_eq!(remaining, request_data);
    }

    #[test]
    fn test_request_message_new_valid() {
        let mut buf = Vec::new();
        let request_data = b"test_payload";

        serialize_request_message(&mut buf, 456, -3, "test_handler", request_data);

        let request_message = FanoutMessage::new(&buf).unwrap();

        assert_eq!(request_message.request_id, 456);
        assert_eq!(request_message.db, -3);
        assert_eq!(request_message.handler, "test_handler");
        assert_eq!(request_message.buf, request_data);
    }

    #[test]
    fn test_request_message_new_invalid_header() {
        let buf = b"invalid_data";

        let result = FanoutMessage::new(buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_cluster_message_header_roundtrip_property_based() {
        // Property-based test: any valid ClusterMessageHeader should roundtrip,
        // including various handler strings.
        let test_cases = vec![
            (0, 0, i32::MIN, 0, ""),
            (1, 1, -1, 1, "handler1"),
            (u16::MAX, u64::MAX, i32::MAX, u16::MAX, "h"),
            (42, 1234567890, 0, 999, "property_based_test"),
            (100, 555, -42, 200, "with_special_字符"),
            (2, 3, 4, 5, "another_handler"),
        ];

        for (version, request_id, db, reserved, handler) in test_cases {
            let original_header = FanoutMessageHeader {
                version,
                request_id,
                db,
                handler: handler.to_string(),
                reserved,
            };

            let mut buf = Vec::new();
            original_header.serialize(&mut buf);

            let (deserialized_header, remaining_buf) =
                FanoutMessageHeader::deserialize(&buf).unwrap();

            assert_eq!(deserialized_header.version, original_header.version);
            assert_eq!(deserialized_header.request_id, original_header.request_id);
            assert_eq!(deserialized_header.db, original_header.db);
            assert_eq!(deserialized_header.reserved, original_header.reserved);
            assert_eq!(deserialized_header.handler, original_header.handler);
            assert_eq!(remaining_buf.len(), 0);
        }
    }
}
