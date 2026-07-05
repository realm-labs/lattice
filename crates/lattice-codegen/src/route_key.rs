use prost_types::field_descriptor_proto::Type as FieldType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtoRouteKeyOption {
    pub actor_kind: String,
    pub key_field: String,
    pub key_type: RouteKeyType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteKeyType {
    U64,
    I64,
    String,
    Bytes,
}

impl RouteKeyType {
    pub(crate) fn from_field_type(field_type: FieldType) -> Option<Self> {
        match field_type {
            FieldType::Uint64 | FieldType::Fixed64 | FieldType::Sfixed64 => Some(Self::U64),
            FieldType::Int64 | FieldType::Sint64 => Some(Self::I64),
            FieldType::String => Some(Self::String),
            FieldType::Bytes => Some(Self::Bytes),
            _ => None,
        }
    }
}
