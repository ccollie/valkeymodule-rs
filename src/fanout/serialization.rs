use crate::ValkeyResult;

/// A trait for types that can be serialized to and deserialized from a byte stream.
///
/// The generic parameter `T` represents the type that will be produced when deserializing.
/// This is typically the implementing type itself but allows for flexibility when needed.
pub trait Serialized {
    /// Serializes the implementing type to the provided writer.
    fn serialize(&self, dest: &mut Vec<u8>);
}

pub trait Deserialized: Sized {
    /// Deserializes an instance of type `T` from the provided buffer.
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self>;
}

/// A convenience trait that combines both `Serialized` and `Deserialized`.
/// This trait is implemented for types that can be both serialized and deserialized.
pub trait Serializable: Serialized + Deserialized {
    
}