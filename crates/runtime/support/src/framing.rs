use anyhow::{Context, Result};
use selium_abi::{decode_frame_len, encode_frame};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Write a length-prefixed frame to an async writer.
pub async fn write_framed<W>(writer: &mut W, payload: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let frame = encode_frame(payload)?;
    writer.write_all(&frame).await.context("write frame")
}

/// Read a length-prefixed frame from an async reader.
pub async fn read_framed<R>(reader: &mut R) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut prefix = [0u8; 4];
    reader
        .read_exact(&mut prefix)
        .await
        .context("read frame length")?;
    let len = decode_frame_len(&prefix).context("decode frame length")?;
    let mut buffer = vec![0u8; len];
    reader
        .read_exact(&mut buffer)
        .await
        .context("read frame payload")?;
    Ok(buffer)
}
