//! Asynchronous TCP server speaking the MongoDB Wire Protocol.
//!
//! This module owns the connection lifecycle: accept a client, spawn a
//! Tokio task, read OP_MSG frames, hand them to the storage router and
//! write the reply back. Raw protocol parsing lives in
//! [`crate::protocol`]; this file only handles I/O.

use std::sync::atomic::{AtomicI32, Ordering};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;

use crate::logger::LOGGER;
use crate::protocol;
use crate::storage::PartitionRouter;
use crate::types::PartitionResult;

mod wire {
    //! Fixed offsets of the 16-byte MongoDB Wire Protocol header.

    /// Fixed size of the standard MongoDB Wire Protocol header (4 fields of 4 bytes each).
    pub const HEADER_SIZE: usize = 16;

    /// Total message length, including the header itself (int32 big-endian).
    pub const MESSAGE_LENGTH_OFFSET: usize = 0;
    /// The requestId of the current message (int32 big-endian).
    pub const REQUEST_ID_OFFSET: usize = 4;
    /// The requestId this message responds to (int32 big-endian).
    pub const RESPONSE_TO_OFFSET: usize = 8;
    /// The operation code to execute (int32 big-endian).
    pub const OP_CODE_OFFSET: usize = 12;

    /// OP_MSG operation code of the MongoDB Wire Protocol.
    pub const OP_MSG: i32 = 2013;

    #[derive(Debug, Clone, Copy)]
    pub struct MessageHeader {
        /// Total header length in bytes (includes the header).
        pub message_length: u32,
        /// The requestId of this message.
        pub request_id: i32,
        /// The requestId this message responds to.
        pub response_to: i32,
        /// The operation code.
        pub op_code: i32,
    }

    impl MessageHeader {
        /// Parses a 16-byte header read from the TCP stream.
        ///
        /// The fields are read in big-endian, as specified by the
        /// MongoDB Wire Protocol.
        pub fn parse(bytes: &[u8; HEADER_SIZE]) -> Self {
            Self {
                message_length: u32::from_be_bytes(
                    bytes[MESSAGE_LENGTH_OFFSET..MESSAGE_LENGTH_OFFSET + 4]
                        .try_into()
                        .expect("4 bytes at MESSAGE_LENGTH_OFFSET"),
                ),
                request_id: i32::from_be_bytes(
                    bytes[REQUEST_ID_OFFSET..REQUEST_ID_OFFSET + 4]
                        .try_into()
                        .expect("4 bytes at REQUEST_ID_OFFSET"),
                ),
                response_to: i32::from_be_bytes(
                    bytes[RESPONSE_TO_OFFSET..RESPONSE_TO_OFFSET + 4]
                        .try_into()
                        .expect("4 bytes at RESPONSE_TO_OFFSET"),
                ),
                op_code: i32::from_be_bytes(
                    bytes[OP_CODE_OFFSET..OP_CODE_OFFSET + 4]
                        .try_into()
                        .expect("4 bytes at OP_CODE_OFFSET"),
                ),
            }
        }
    }
}

const KOVA_PORT: u16 = 27017;

/// Monotonic counter used for the `requestId` of our own replies.
static REPLY_REQUEST_ID: AtomicI32 = AtomicI32::new(1);

/// Starts the Kova TCP network server.
///
/// Connections are accepted asynchronously; each client runs on its own
/// Tokio task. Commands are forwarded to `router`, whose partition
/// workers execute them on dedicated OS threads.
///
/// ## Port
///
/// The default port is `27017` (the standard MongoDB port).
pub async fn start_server(router: PartitionRouter) -> std::io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", KOVA_PORT)).await?;
    LOGGER.info(format!("Network server started on port {}", KOVA_PORT));

    loop {
        let (mut socket, addr) = listener.accept().await?;
        LOGGER.info(format!("Connection received from {}", addr));

        // Each client gets its own task; `Arc` lets every task share
        // the same router without cloning worker channels individually.
        let client_router = router.clone();
        spawn(async move {
            if let Err(err) = handle_client(&mut socket, &client_router).await {
                match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        LOGGER.info(format!("Client {} disconnected", addr));
                    }
                    _ => LOGGER.error(format!("Error processing client {}: {}", addr, err)),
                }
            }
        });
    }
}

/// Reads OP_MSG frames from one client until it disconnects.
///
/// Each iteration reads the 16-byte header, then the remaining
/// `messageLength - 16` bytes, decodes the OP_MSG body, executes the
/// command through the router and writes the reply.
async fn handle_client(socket: &mut TcpStream, router: &PartitionRouter) -> std::io::Result<()> {
    loop {
        // ---- header ------------------------------------------------
        // `with_capacity` only reserves memory; the length must be set
        // explicitly so `read_exact` actually fills the 16 bytes.
        let mut header_buf = BytesMut::with_capacity(wire::HEADER_SIZE);
        header_buf.resize(wire::HEADER_SIZE, 0);

        match socket.read_exact(&mut header_buf).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "socket unexpectedly closed by client",
                ));
            }
            Err(err) => return Err(err),
        }

        let header = wire::MessageHeader::parse(
            header_buf
                .as_ref()
                .try_into()
                .expect("exactly 16 bytes were read"),
        );

        // ---- body --------------------------------------------------
        let body_len = header
            .message_length
            .saturating_sub(wire::HEADER_SIZE as u32) as usize;

        if body_len > 0 {
            let mut body_buf = BytesMut::with_capacity(body_len);
            body_buf.resize(body_len, 0);
            socket.read_exact(&mut body_buf).await?;

            if header.op_code == wire::OP_MSG {
                LOGGER.debug(format!(
                    "OP_MSG received (requestId={}, {} bytes)",
                    header.request_id, header.message_length
                ));

                if let Err(err) = process_op_msg(&header, &body_buf, router, socket).await {
                    LOGGER.warn(format!("Failed to process OP_MSG: {}", err));
                }
            } else {
                LOGGER.warn(format!(
                    "Unsupported opCode {} (only OP_MSG is handled)",
                    header.op_code
                ));
            }
        }
    }
}

/// Decodes, executes and replies to a single OP_MSG.
async fn process_op_msg(
    header: &wire::MessageHeader,
    body: &[u8],
    router: &PartitionRouter,
    socket: &mut TcpStream,
) -> Result<(), String> {
    let msg = protocol::decode_op_msg(body)?;

    // Handshake commands (`ping`, `hello`, `isMaster`) are answered
    // here without touching storage.
    let reply_body = if let Some(direct) = protocol::special_command(&msg.command) {
        LOGGER.debug(format!(
            "Handshake command answered directly: {:?}",
            msg.command.keys().collect::<Vec<_>>()
        ));
        direct
    } else {
        match protocol::command_from_document(&msg.command) {
            Ok(command) => match router.dispatch(command) {
                Some(rx) => match rx.await {
                    Ok(result) => protocol::reply_document(&result),
                    Err(_) => protocol::reply_document(&PartitionResult::Error {
                        message: "partition worker dropped the request".into(),
                    }),
                },
                None => protocol::reply_document(&PartitionResult::Error {
                    message: "storage partition unavailable".into(),
                }),
            },
            Err(message) => {
                LOGGER.warn(format!("Command rejected: {}", message));
                bson::doc! { "ok": 0.0, "errmsg": message }
            }
        }
    };

    let request_id = REPLY_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let frame = protocol::encode_reply(request_id, header.request_id, &reply_body)?;

    socket
        .write_all(&frame)
        .await
        .map_err(|err| format!("failed to write reply: {}", err))?;

    LOGGER.info(format!(
        "Replied to requestId {} (reply requestId {})",
        header.request_id, request_id
    ));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire::{MessageHeader, HEADER_SIZE, OP_MSG};

    /// Builds a 16-byte big-endian header with the given fields.
    fn build_header(
        message_length: u32,
        request_id: i32,
        response_to: i32,
        op_code: i32,
    ) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        bytes[0..4].copy_from_slice(&message_length.to_be_bytes());
        bytes[4..8].copy_from_slice(&request_id.to_be_bytes());
        bytes[8..12].copy_from_slice(&response_to.to_be_bytes());
        bytes[12..16].copy_from_slice(&op_code.to_be_bytes());
        bytes
    }

    #[test]
    fn test_parse_op_msg_header() {
        let bytes = build_header(32, 1, 0, OP_MSG);
        let header = MessageHeader::parse(&bytes);

        assert_eq!(header.message_length, 32);
        assert_eq!(header.request_id, 1);
        assert_eq!(header.response_to, 0);
        assert_eq!(header.op_code, OP_MSG);
    }

    #[test]
    fn test_parse_unknown_opcode() {
        let bytes = build_header(16, 42, 7, 2004); // OP_REPLY
        let header = MessageHeader::parse(&bytes);

        assert_eq!(header.op_code, 2004);
        assert_ne!(header.op_code, OP_MSG);
        assert_eq!(header.request_id, 42);
        assert_eq!(header.response_to, 7);
    }

    #[test]
    fn test_parse_preserves_big_endian_layout() {
        let bytes = build_header(256, 0, 0, OP_MSG);
        assert_eq!(&bytes[0..4], &[0x00, 0x00, 0x01, 0x00]);

        let header = MessageHeader::parse(&bytes);
        assert_eq!(header.message_length, 256);
    }

    #[test]
    fn test_header_size_is_16_bytes() {
        assert_eq!(HEADER_SIZE, 16);
        assert_eq!(build_header(0, 0, 0, 0).len(), HEADER_SIZE);
    }

    /// Connects to `addr`, writes one frame and reads the reply.
    ///
    /// Returns the decoded OP_MSG reply together with its header.
    async fn send_and_read_reply(
        addr: std::net::SocketAddr,
        frame: Vec<u8>,
    ) -> (protocol::OpMsg, MessageHeader) {
        let mut client = TcpStream::connect(addr).await.expect("connect");
        client.write_all(&frame).await.expect("write request");

        let mut header_buf = [0u8; wire::HEADER_SIZE];
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read_exact(&mut header_buf),
        )
        .await
        .expect("reply timed out")
        .expect("reply header read");

        let header = MessageHeader::parse(&header_buf);
        assert_eq!(header.op_code, OP_MSG, "reply must be OP_MSG");
        assert!(header.message_length > wire::HEADER_SIZE as u32);

        let body_len = header.message_length as usize - wire::HEADER_SIZE;
        let mut body = vec![0u8; body_len];
        client.read_exact(&mut body).await.expect("reply body");

        let reply = protocol::decode_op_msg(&body).expect("decode reply");
        (reply, header)
    }

    #[tokio::test]
    async fn test_server_accepts_insert_and_replies() {
        let router = PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test port");
        let addr = listener.local_addr().expect("local addr");

        let server_router = router.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let _ = handle_client(&mut socket, &server_router).await;
        });

        // Build an OP_MSG frame carrying an insert command.
        let command = bson::doc! { "insert": "users", "documents": [{ "name": "Ada" }] };
        let frame = protocol::encode_reply(1, 0, &command).expect("encode");

        let mut client = TcpStream::connect(addr).await.expect("connect");
        client.write_all(&frame).await.expect("write request");

        // Read the 16-byte reply header.
        let mut header_buf = [0u8; wire::HEADER_SIZE];
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read_exact(&mut header_buf),
        )
        .await
        .expect("reply timed out")
        .expect("reply header read");

        let reply_header = MessageHeader::parse(&header_buf);
        assert_eq!(reply_header.op_code, OP_MSG);
        assert_eq!(
            reply_header.response_to, 1,
            "reply must echo the request id"
        );
        assert!(reply_header.message_length > wire::HEADER_SIZE as u32);

        // Read and decode the reply body.
        let body_len = reply_header.message_length as usize - wire::HEADER_SIZE;
        let mut body = vec![0u8; body_len];
        client.read_exact(&mut body).await.expect("reply body");

        let msg = protocol::decode_op_msg(&body).expect("decode reply");
        assert_eq!(
            msg.command.get_f64("ok").expect("ok field"),
            1.0,
            "insert must succeed: {:?}",
            msg.command
        );
        assert_eq!(msg.command.get_i64("n").expect("n field"), 1);
    }

    #[tokio::test]
    async fn test_server_replies_to_ping_handshake() {
        let router = PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test port");
        let addr = listener.local_addr().expect("local addr");

        let server_router = router.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let _ = handle_client(&mut socket, &server_router).await;
        });

        let frame = protocol::encode_reply(1, 0, &bson::doc! { "ping": 1.0 }).expect("encode");
        let (reply, _) = send_and_read_reply(addr, frame).await;

        assert_eq!(reply.command.get_f64("ok").expect("ok"), 1.0);
    }

    #[tokio::test]
    async fn test_server_replies_to_hello_handshake() {
        let router = PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test port");
        let addr = listener.local_addr().expect("local addr");

        let server_router = router.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let _ = handle_client(&mut socket, &server_router).await;
        });

        let frame = protocol::encode_reply(1, 0, &bson::doc! { "hello": 1.0 }).expect("encode");
        let (reply, header) = send_and_read_reply(addr, frame).await;

        assert_eq!(header.response_to, 1, "reply must echo requestId");
        assert_eq!(reply.command.get_f64("ok").expect("ok"), 1.0);
        assert_eq!(
            reply
                .command
                .get_bool("isWritablePrimary")
                .expect("primary"),
            true
        );
        assert_eq!(reply.command.get_bool("helloOk").expect("helloOk"), true);
    }

    #[tokio::test]
    async fn test_server_reports_unknown_command() {
        let router = PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test port");
        let addr = listener.local_addr().expect("local addr");

        let server_router = router.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let _ = handle_client(&mut socket, &server_router).await;
        });

        let command = bson::doc! { "drop": "users" };
        let frame = protocol::encode_reply(1, 0, &command).expect("encode");

        let mut client = TcpStream::connect(addr).await.expect("connect");
        client.write_all(&frame).await.expect("write request");

        let mut header_buf = [0u8; wire::HEADER_SIZE];
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read_exact(&mut header_buf),
        )
        .await
        .expect("reply timed out")
        .expect("reply header read");

        let reply_header = MessageHeader::parse(&header_buf);
        let body_len = reply_header.message_length as usize - wire::HEADER_SIZE;
        let mut body = vec![0u8; body_len];
        client.read_exact(&mut body).await.expect("reply body");

        let msg = protocol::decode_op_msg(&body).expect("decode reply");
        assert_eq!(msg.command.get_f64("ok").expect("ok field"), 0.0);
        assert!(
            msg.command
                .get_str("errmsg")
                .expect("errmsg")
                .contains("drop"),
            "error must name the command: {:?}",
            msg.command
        );
    }
}
