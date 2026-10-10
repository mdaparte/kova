mod wire {
    /// Fixed size of the standard MongoDB Wire Protocol header (4 fields of 4 bytes each).
    pub const HEADER_SIZE: usize = 16;

    /// Total message length, including the header itself (int32 big-endian).
    pub const MESSAGE_LENGTH_OFFSET: usize = 0;
    /// The requestId of the current message (int32 big-endian).
    pub const REQUEST_ID_OFFSET: usize = 4;
    /// The requestId this message is responding to (int32 big-endian).
    pub const RESPONSE_TO_OFFSET: usize = 8;
    /// The operation code to execute (int32 big-endian).
    pub const OP_CODE_OFFSET: usize = 12;

    /// OP_MSG operation code of the MongoDB Wire Protocol.
    pub const OP_MSG: i32 = 2013;

    #[derive(Debug, Clone, Copy)]
    pub struct MessageHeader {
        /// Total header length in bytes (includes the header).
        pub message_length: u32,
        /// requestId da mensagem.
        pub request_id: i32,
        /// responseTo da mensagem.
        pub response_to: i32,
        /// Código da operação.
        pub op_code: i32,
    }

    impl MessageHeader {
        /// Parses a 16-byte header read from the TCP stream.
        ///
        /// The fields are read in big-endian, as specified by the protocol
        /// of the MongoDB Wire Protocol.
        pub fn parse(bytes: &[u8; HEADER_SIZE]) -> Option<Self> {
            if bytes.len() != HEADER_SIZE {
                return None;
            }

            Some(Self {
                message_length: u32::from_be_bytes(
                    bytes[MESSAGE_LENGTH_OFFSET..MESSAGE_LENGTH_OFFSET + 4]
                        .try_into()
                        .ok()?,
                ),
                request_id: i32::from_be_bytes(
                    bytes[REQUEST_ID_OFFSET..REQUEST_ID_OFFSET + 4]
                        .try_into()
                        .ok()?,
                ),
                response_to: i32::from_be_bytes(
                    bytes[RESPONSE_TO_OFFSET..RESPONSE_TO_OFFSET + 4]
                        .try_into()
                        .ok()?,
                ),
                op_code: i32::from_be_bytes(
                    bytes[OP_CODE_OFFSET..OP_CODE_OFFSET + 4].try_into().ok()?,
                ),
            })
        }
    }
}

use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::spawn;

use logger::LOGGER;

const KOVA_PORT: u16 = 27017;

/// Starts the Kova TCP network server.
///
/// The server accepts concurrent connections, each scheduled on its own
/// Tokio asynchronous task (`tokio::spawn`). For each client, the MongoDB
/// Wire Protocol header is read and, if the operation code is
/// `OP_MSG` (2013), a log is printed to indicate a valid MongoDB message.
///
/// ## Port
///
/// The default port is `27017` (the standard MongoDB port). It can be
/// overridden by passing the `KOVA_PORT` environment variable to the process.
pub async fn start_server() -> std::io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", KOVA_PORT)).await?;
    LOGGER.info(&format!("Network server started on port {}", KOVA_PORT));
    LOGGER.debug("Network server listening for incoming connections");

    // Aceita conexões de forma assíncrona e concorrente, escalonando cada
    // cliente em uma própria tarefa do Tokio.
    loop {
        let (mut socket, addr) = listener.accept().await?;
        LOGGER.info(&format!("Connection received from {}", addr));
        LOGGER.debug(&format!("Client {} connected", addr));

        spawn(async move {
            if let Err(err) = handle_client(&mut socket).await {
                LOGGER.error(&format!("Error processing client {}: {}", addr, err));
            }
        });
    }
}

/// Handles a single client, reading the MongoDB Wire Protocol header.
///
/// The read is performed on a `BytesMut` buffer of 16 bytes that contains
/// the four 4-byte fields of the header: `messageLength` (int32),
/// `requestId` (int32), `responseTo` (int32) and `opCode` (int32), all
/// stored in big-endian (network byte order).
///
/// If the `opCode` is `OP_MSG` (2013), the system prints a log indicating
/// a valid MongoDB message was received.
///
/// Adds basic error handling for unexpected socket closures, returning
/// `std::io::ErrorKind::UnexpectedEof` when the client terminates the
/// connection.
async fn handle_client(socket: &mut tokio::net::TcpStream) -> std::io::Result<()> {
    let mut buffer = BytesMut::with_capacity(wire::HEADER_SIZE);

    // Blocks until at least 16 bytes are available on the stream.
    // This is required to ensure the full header was received before
    // it is parsed.
    if socket.read_exact(&mut buffer).await.is_err() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "socket unexpectedly closed by client",
        ));
    }

    // The header is composed of 4 fields of 4 bytes each (big-endian).
    let header_bytes = buffer.split_to(wire::HEADER_SIZE).as_ref().to_vec();

    let header = wire::MessageHeader::parse(
        header_bytes
            .as_slice()
            .try_into()
            .expect("header with 16 bytes must always be parseable"),
    )
    .expect("header with 16 bytes must always be parseable");

    if header.op_code == wire::OP_MSG {
        eprintln!("[Kova] Valid MongoDB message received");
    }

    Ok(())
}
