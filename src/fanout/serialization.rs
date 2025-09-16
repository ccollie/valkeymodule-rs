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

/// Trait that must be implemented for Request and Response types to be used as
/// messages in the fanout system.
pub trait Serializable: Serialized + Deserialized {
    
}