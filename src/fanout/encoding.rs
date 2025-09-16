use std::fmt;

/// Decoding varint error.
#[derive(Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DecodeError {
    /// The buffer does not contain a valid LEB128 encoding.
    Overflow,
    /// The buffer does not contain enough data to decode.
    InsufficientData {
        /// Requested number of bytes to decode the value.
        requested: usize,
        /// The number of bytes available in the buffer.
        available: usize,
    },
    /// A custom error message.
    Other(std::borrow::Cow<'static, str>),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Overflow => write!(f, "decoded value would overflow the target type"),
            DecodeError::InsufficientData { available, requested } => write!(
                f,
                "not enough bytes to decode value: only {} were available, but {} requested",
                available, requested
            ),
            DecodeError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl DecodeError {
    /// Creates a new `DecodeError::Overflow` indicating that the decoded value would overflow the target type.
    #[inline]
    pub const fn overflow() -> Self {
        Self::Overflow
    }

    /// Creates a new `DecodeError::InsufficientData` indicating that the buffer does not have enough data
    /// to decode a value.
    #[inline]
    pub const fn insufficient_data(available: usize, requested: usize) -> Self {
        Self::InsufficientData {
            available,
            requested,
        }
    }

    /// Creates a new `DecodeError::Other` with the given message.
    #[inline]
    pub fn other(msg: impl Into<std::borrow::Cow<'static, str>>) -> Self {
        Self::Other(msg.into())
    }
}

pub type DecodeResult<T> = Result<T, DecodeError>;

pub(crate) fn write_u64_le(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn try_read_u64_le(buf: &mut &[u8]) -> DecodeResult<u64> {
    const U64_BYTE_SIZE: usize = size_of::<u64>();

    if buf.len() < U64_BYTE_SIZE {
        return Err(DecodeError::insufficient_data(buf.len(), U64_BYTE_SIZE));
    }
    let mut array = [0u8; U64_BYTE_SIZE];
    array.copy_from_slice(&buf[..U64_BYTE_SIZE]);
    *buf = &buf[U64_BYTE_SIZE..];
    Ok(u64::from_le_bytes(array))
}

/// Writes an unsigned varint to the buffer
pub(crate) fn write_uvarint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Writes a signed varint using zigzag encoding
pub(crate) fn write_signed_varint(buf: &mut Vec<u8>, value: i64) {
    // Use zigzag encoding for signed values
    let unsigned = zigzag_encode(value);
    write_uvarint(buf, unsigned);
}

pub(crate) fn write_byte_slice(buf: &mut Vec<u8>, slice: &[u8]) {
    buf.reserve(slice.len() + 3);
    write_uvarint(buf, slice.len() as u64);
    if slice.is_empty() {
        return;
    }
    buf.extend_from_slice(slice);
}

/// Reads an unsigned varint from the buffer
/// Returns the value and the number of bytes consumed, or None if invalid
pub(crate) fn try_read_uvarint(buf: &mut &[u8]) -> DecodeResult<u64> {
    let mut value: u64 = 0;
    let mut shift = 0;
    let mut current_offset = 0;

    loop {
        if current_offset >= buf.len() {
            return Err(DecodeError::Overflow); // Unexpected end of buffer
        }

        let byte = buf[current_offset];
        current_offset += 1;

        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift > 63 {
            // Protect against malicious inputs
            return Err(DecodeError::Overflow);
        }
    }
    *buf = &buf[current_offset..];

    Ok(value)
}

/// Reads a signed varint from the buffer using zigzag encoding
pub(crate) fn try_read_signed_varint(buf: &mut &[u8]) -> DecodeResult<i64> {
    try_read_uvarint(buf).map(zigzag_decode)
}

pub(crate) fn try_read_byte_slice<'a>(buf: &mut &'a [u8]) -> DecodeResult<&'a [u8]> {
    let len = try_read_uvarint(buf)? as usize;

    if len > buf.len() {
        return Err(DecodeError::insufficient_data(buf.len(), len)); // Not enough data
    }

    let slice = &buf[..len];
    *buf = &buf[len..];
    Ok(slice)
}

pub(crate) fn write_f64_le(buf: &mut Vec<u8>, value: f64) {
    let bits = value.to_bits();
    write_u64_le(buf, bits)
}

#[inline]
pub(crate) fn try_read_f64_le(buf: &mut &[u8]) -> DecodeResult<f64> {
    try_read_u64_le(buf).map(f64::from_bits)
}

// see: http://stackoverflow.com/a/2211086/56332
// casting required because operations like unary negation
// cannot be performed on unsigned integers
#[inline]
pub(crate) fn zigzag_decode(from: u64) -> i64 {
    ((from >> 1) ^ (-((from & 1) as i64)) as u64) as i64
}

#[inline]
pub(crate) fn zigzag_encode(from: i64) -> u64 {
    ((from << 1) ^ (from >> 63)) as u64
}
