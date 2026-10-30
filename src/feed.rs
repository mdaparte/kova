//! Change feed.
//!
//! Off unless `[feed] enabled` is true. A committed insert, update, or
//! delete then publishes one JSON record to Apache Iggy. Iggy is another
//! process. This module is a client of it. A follower and a shard router
//! do not publish. If Iggy is down, the database write still stands and
//! the same record is sent again.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde_json::{json, Value};

use std::str::FromStr;

use iggy::prelude::{Client, IggyClient, IggyMessage};

use crate::config::{Config, FeedConfig};
use crate::logger::LOGGER;

/// One committed document change. `before` is empty on insert. `after` is
/// empty on delete. `ns`, `id`, and `lsn` identify a repeat.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Record {
    pub op: String,
    pub ns: String,
    pub id: String,
    pub before: Value,
    pub after: Value,
    pub lsn: u64,
}

impl Record {
    pub fn new(
        op: impl Into<String>,
        ns: impl Into<String>,
        id: impl Into<String>,
        before: Value,
        after: Value,
        lsn: u64,
    ) -> Self {
        Self {
            op: op.into(),
            ns: ns.into(),
            id: id.into(),
            before,
            after,
            lsn,
        }
    }
}

/// JSON object for a stored document. An absent side is `{}`.
pub fn document_json(document: &bson::Document) -> Value {
    serde_json::to_value(document).unwrap_or_else(|_| json!({}))
}

pub fn empty_document() -> Value {
    json!({})
}

/// Where committed records go. `None` means the feed is off.
pub struct Feed {
    kind: Kind,
}

enum Kind {
    Memory(Mutex<Vec<Record>>),
    Live(Live),
}

struct Live {
    tx: crossbeam_channel::Sender<Vec<u8>>,
    rx: Mutex<Option<crossbeam_channel::Receiver<Vec<u8>>>>,
    leader: Mutex<Arc<dyn Fn() -> bool + Send + Sync>>,
    armed: AtomicBool,
    accepted: AtomicU64,
    config: FeedConfig,
}

impl Feed {
    /// A feed that keeps records in memory. Tests read them back.
    pub fn memory() -> Arc<Self> {
        Arc::new(Self {
            kind: Kind::Memory(Mutex::new(Vec::new())),
        })
    }

    fn live(config: FeedConfig) -> Arc<Self> {
        let (tx, rx) = crossbeam_channel::unbounded();
        Arc::new(Self {
            kind: Kind::Live(Live {
                tx,
                rx: Mutex::new(Some(rx)),
                leader: Mutex::new(Arc::new(|| false)),
                armed: AtomicBool::new(false),
                accepted: AtomicU64::new(0),
                config,
            }),
        })
    }

    /// Starts the publisher. `leader` is true on a standalone process and
    /// on the primary. A follower passes a check that returns false.
    pub fn arm(self: &Arc<Self>, leader: Arc<dyn Fn() -> bool + Send + Sync>) {
        let Kind::Live(live) = &self.kind else {
            return;
        };
        *live.leader.lock().expect("feed leader") = leader;
        live.armed.store(true, Ordering::Release);
        let Some(rx) = live.rx.lock().expect("feed rx").take() else {
            return;
        };
        let config = live.config.clone();
        let runtime = tokio::runtime::Handle::current();
        std::thread::Builder::new()
            .name("kova-feed".into())
            .spawn(move || {
                runtime.block_on(publish_loop(config, rx));
            })
            .expect("feed thread");
    }

    /// Marks the live feed open without connecting. Tests use this to
    /// check the leader gate.
    pub fn arm_without_connection(&self, leader: bool) {
        let Kind::Live(live) = &self.kind else {
            return;
        };
        *live.leader.lock().expect("feed leader") = Arc::new(move || leader);
        live.armed.store(true, Ordering::Release);
    }

    pub fn submit(&self, records: Vec<Record>) {
        if records.is_empty() {
            return;
        }
        match &self.kind {
            Kind::Memory(records_out) => {
                records_out.lock().expect("feed records").extend(records);
            }
            Kind::Live(live) => {
                if !live.armed.load(Ordering::Acquire) {
                    return;
                }
                let allow = live.leader.lock().expect("feed leader")();
                if !allow {
                    return;
                }
                for record in records {
                    let bytes = serde_json::to_vec(&record).unwrap_or_default();
                    if live.tx.send(bytes).is_ok() {
                        live.accepted.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    pub fn records(&self) -> Vec<Record> {
        match &self.kind {
            Kind::Memory(records) => records.lock().expect("feed records").clone(),
            Kind::Live(_) => Vec::new(),
        }
    }

    pub fn accepted(&self) -> u64 {
        match &self.kind {
            Kind::Memory(records) => records.lock().expect("feed records").len() as u64,
            Kind::Live(live) => live.accepted.load(Ordering::Relaxed),
        }
    }
}

/// `None` when the section is absent, `enabled` is false, or this process
/// is a shard router. A router forwards the command and does not publish.
pub fn open(config: &Config) -> Option<Arc<Feed>> {
    if !config.feed.enabled {
        return None;
    }
    if config.sharding.enabled && config.sharding.role == "router" {
        return None;
    }
    Some(Feed::live(config.feed.clone()))
}

/// Sends queued payloads. A failure leaves them at the front so the next
/// attempt sends the same bytes.
pub async fn drain<T: Transport>(
    transport: &mut T,
    pending: &mut VecDeque<Vec<u8>>,
) -> Result<(), String> {
    while let Some(payload) = pending.pop_front() {
        if let Err(message) = transport.publish(&payload).await {
            pending.push_front(payload);
            return Err(message);
        }
    }
    Ok(())
}

pub trait Transport {
    fn publish(
        &mut self,
        payload: &[u8],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>>;
}

async fn publish_loop(config: FeedConfig, rx: crossbeam_channel::Receiver<Vec<u8>>) {
    let mut pending = VecDeque::new();
    loop {
        if pending.is_empty() {
            match rx.recv() {
                Ok(payload) => pending.push_back(payload),
                Err(_) => return,
            }
        }
        while let Ok(payload) = rx.try_recv() {
            pending.push_back(payload);
        }
        CONNECTS.fetch_add(1, Ordering::Relaxed);
        match iggy_session(&config, &mut pending).await {
            Ok(()) => {}
            Err(message) => {
                LOGGER.warn(format!("feed publish failed: {message}"));
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

static CONNECTS: AtomicU64 = AtomicU64::new(0);

/// How many times this process has tried to open Iggy. Stays at 0 when
/// the feed is off.
pub fn connections() -> u64 {
    CONNECTS.load(Ordering::Relaxed)
}

async fn iggy_session(config: &FeedConfig, pending: &mut VecDeque<Vec<u8>>) -> Result<(), String> {
    let client = IggyClient::from_connection_string(&config.connection_string())
        .map_err(|err| err.to_string())?;
    client.connect().await.map_err(|err| err.to_string())?;
    let producer = client
        .producer(config.stream.as_str(), config.topic.as_str())
        .map_err(|err| err.to_string())?
        .build();
    producer.init().await.map_err(|err| err.to_string())?;
    let mut session = IggySession { producer };
    let result = drain(&mut session, pending).await;
    let _ = client.shutdown().await;
    result
}

struct IggySession {
    producer: iggy::clients::producer::IggyProducer,
}

impl Transport for IggySession {
    fn publish(
        &mut self,
        payload: &[u8],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
        let payload = payload.to_vec();
        Box::pin(async move {
            let text = std::str::from_utf8(&payload).map_err(|err| err.to_string())?;
            let message = IggyMessage::from_str(text).map_err(|err| err.to_string())?;
            self.producer
                .send(vec![message])
                .await
                .map_err(|err| err.to_string())?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Flaky {
        fails_left: usize,
        sent: Vec<Vec<u8>>,
    }

    impl Transport for Flaky {
        fn publish(
            &mut self,
            payload: &[u8],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>>
        {
            let fail = self.fails_left > 0;
            if fail {
                self.fails_left -= 1;
            } else {
                self.sent.push(payload.to_vec());
            }
            Box::pin(async move {
                if fail {
                    Err("iggy is down".into())
                } else {
                    Ok(())
                }
            })
        }
    }

    #[test]
    fn disabled_feed_does_not_open() {
        let config = Config::default();
        assert!(!config.feed.enabled);
        assert!(open(&config).is_none());
        assert_eq!(connections(), 0);
    }

    #[test]
    fn a_router_does_not_open_a_feed() {
        let mut config = Config::default();
        config.feed.enabled = true;
        config.sharding.enabled = true;
        config.sharding.role = "router".into();
        assert!(open(&config).is_none());
    }

    #[test]
    fn a_follower_does_not_enqueue() {
        let mut config = Config::default();
        config.feed.enabled = true;
        let feed = Feed::live(config.feed);
        feed.arm_without_connection(false);
        feed.submit(vec![Record::new(
            "insert",
            "db.C1",
            "c1",
            empty_document(),
            json!({ "A": 1 }),
            4,
        )]);
        assert_eq!(feed.accepted(), 0);
        feed.arm_without_connection(true);
        feed.submit(vec![Record::new(
            "insert",
            "db.C1",
            "c1",
            empty_document(),
            json!({ "A": 1 }),
            4,
        )]);
        assert_eq!(feed.accepted(), 1);
    }

    #[tokio::test]
    async fn a_failed_send_retries_the_same_bytes() {
        let payload = br#"{"op":"insert","ns":"db.C1","id":"c1","lsn":4}"#.to_vec();
        let mut pending = VecDeque::from([payload.clone()]);
        let mut transport = Flaky {
            fails_left: 1,
            sent: Vec::new(),
        };
        let err = drain(&mut transport, &mut pending).await.unwrap_err();
        assert!(err.contains("down"), "{err}");
        assert_eq!(pending.front().map(Vec::as_slice), Some(payload.as_slice()));
        drain(&mut transport, &mut pending).await.expect("retry");
        assert!(pending.is_empty());
        assert_eq!(transport.sent, vec![payload]);
    }
}
