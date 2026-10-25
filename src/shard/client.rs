//! One OP_MSG connection per shard. A dropped socket is opened again
//! on the next command.

use std::sync::atomic::{AtomicI32, Ordering};

use async_trait::async_trait;
use bson::Document;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use super::ShardLink;

pub struct TcpShards {
    addresses: Vec<String>,
    connections: Vec<Mutex<Option<TcpStream>>>,
    next_id: AtomicI32,
}

impl TcpShards {
    pub fn new(addresses: Vec<String>) -> Self {
        let connections = (0..addresses.len()).map(|_| Mutex::new(None)).collect();
        Self {
            addresses,
            connections,
            next_id: AtomicI32::new(1),
        }
    }

    async fn roundtrip(&self, shard: usize, command: &Document) -> Result<Document, String> {
        let address = self
            .addresses
            .get(shard)
            .ok_or_else(|| format!("shard index {shard} is out of range"))?;
        let mut guard = self.connections[shard].lock().await;
        let mut last = format!("shard {address} did not answer");
        for _ in 0..2 {
            if guard.is_none() {
                match TcpStream::connect(address.as_str()).await {
                    Ok(stream) => {
                        let _ = stream.set_nodelay(true);
                        *guard = Some(stream);
                    }
                    Err(err) => {
                        last = format!("shard {address} is not reachable: {err}");
                        continue;
                    }
                }
            }
            let stream = guard.as_mut().expect("connected just above");
            let request_id = self.next_id.fetch_add(1, Ordering::Relaxed);
            match exchange(stream, request_id, command).await {
                Ok(reply) => return Ok(reply),
                Err(message) => {
                    *guard = None;
                    last = format!("shard {address}: {message}");
                }
            }
        }
        Err(last)
    }
}

async fn exchange(
    stream: &mut TcpStream,
    request_id: i32,
    command: &Document,
) -> Result<Document, String> {
    let frame = crate::protocol::encode_reply(request_id, 0, command)?;
    stream
        .write_all(&frame)
        .await
        .map_err(|err| format!("write failed: {err}"))?;
    let mut header = [0u8; crate::protocol::HEADER_SIZE];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|err| format!("read failed: {err}"))?;
    let message_length = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let op_code = i32::from_le_bytes([header[12], header[13], header[14], header[15]]);
    if op_code != crate::protocol::OP_MSG {
        return Err(format!("reply opcode is {op_code}"));
    }
    if message_length < crate::protocol::HEADER_SIZE as u32
        || message_length as usize > crate::protocol::MAX_MESSAGE_SIZE
    {
        return Err(format!("reply length {message_length} is out of range"));
    }
    let mut body = vec![0u8; message_length as usize - crate::protocol::HEADER_SIZE];
    stream
        .read_exact(&mut body)
        .await
        .map_err(|err| format!("read failed: {err}"))?;
    Ok(crate::protocol::decode_op_msg(&body)?.command)
}

#[async_trait]
impl ShardLink for TcpShards {
    async fn call(&self, shard: usize, command: Document) -> Document {
        match self.roundtrip(shard, &command).await {
            Ok(reply) => reply,
            Err(message) => bson::doc! { "ok": 0.0, "errmsg": message },
        }
    }
}
