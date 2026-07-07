use lattice_gateway::error::GatewayError;
use lattice_gateway::frame::{BinaryClientCodec, ClientCodec, ClientFrame};
use prost::Message as ProstMessage;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub async fn read_client_frame<R>(reader: &mut R) -> Result<ClientFrame, TcpFrameError>
where
    R: AsyncRead + Unpin,
{
    let len = reader.read_u32().await? as usize;
    let mut bytes = vec![0; len];
    reader.read_exact(&mut bytes).await?;
    BinaryClientCodec
        .decode(bytes.as_slice())
        .map_err(TcpFrameError::Gateway)
}

pub async fn write_client_frame<W>(writer: &mut W, frame: ClientFrame) -> Result<(), TcpFrameError>
where
    W: AsyncWrite + Unpin,
{
    let bytes = BinaryClientCodec
        .encode(frame)
        .map_err(TcpFrameError::Gateway)?;
    writer.write_u32(bytes.len() as u32).await?;
    writer.write_all(bytes.as_slice()).await?;
    writer.flush().await?;
    Ok(())
}

pub fn request_frame<M>(msg_id: u32, message: &M) -> ClientFrame
where
    M: ProstMessage,
{
    ClientFrame {
        msg_id,
        payload: message.encode_to_vec(),
    }
}

pub fn decode_reply<M>(frame: ClientFrame, expected_msg_id: u32) -> Result<M, TcpFrameError>
where
    M: ProstMessage + Default,
{
    if frame.msg_id != expected_msg_id {
        return Err(TcpFrameError::UnexpectedMessageId {
            expected: expected_msg_id,
            actual: frame.msg_id,
        });
    }
    M::decode(frame.payload.as_slice()).map_err(|error| TcpFrameError::Decode(error.to_string()))
}

#[derive(Debug, thiserror::Error)]
pub enum TcpFrameError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("gateway frame error: {0}")]
    Gateway(GatewayError),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("unexpected msg id: expected {expected}, got {actual}")]
    UnexpectedMessageId { expected: u32, actual: u32 },
}
