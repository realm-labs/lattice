use crate::error::GatewayError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientFrame {
    pub msg_id: u32,
    pub payload: Vec<u8>,
}

pub trait ClientCodec {
    fn decode(&self, bytes: &[u8]) -> Result<ClientFrame, GatewayError>;
    fn encode(&self, frame: ClientFrame) -> Result<Vec<u8>, GatewayError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BinaryClientCodec;

impl ClientCodec for BinaryClientCodec {
    fn decode(&self, bytes: &[u8]) -> Result<ClientFrame, GatewayError> {
        if bytes.len() < 4 {
            return Err(GatewayError::FrameTooShort);
        }

        let msg_id = u32::from_be_bytes(bytes[0..4].try_into().expect("slice length checked"));
        Ok(ClientFrame {
            msg_id,
            payload: bytes[4..].to_vec(),
        })
    }

    fn encode(&self, frame: ClientFrame) -> Result<Vec<u8>, GatewayError> {
        let mut bytes = Vec::with_capacity(4 + frame.payload.len());
        bytes.extend_from_slice(&frame.msg_id.to_be_bytes());
        bytes.extend_from_slice(&frame.payload);
        Ok(bytes)
    }
}
