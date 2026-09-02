use std::pin::Pin;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_stream::try_stream;
use bytes::BytesMut;
use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio::io::{split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_util::codec::{Decoder, Encoder};

use crate::config::configs::LogOnConfig;
use crate::fix::codec::FixCodec;
use crate::fix::messages;
use crate::fix::raw_msg::{RawMessage, ValType};
use crate::fix::tag::{self, msg_type};
use crate::network::{framing, tcpsocket};
use crate::types::types::Tick;
use crate::utils::caster::parse_uint;
use crate::utils::symbol::SymbolInitilizer;

/// How many undelivered ticks the channel will buffer before `send` starts blocking.
const TICK_CHANNEL_CAPACITY: usize = 1024;
const READ_CHUNK_SIZE: usize = 4096;

#[derive(Debug, Serialize, Deserialize)]
pub struct SeqPair {
    outbound_seq: u32,
    inbound_seq: u32,
}

/// Connects to the exchange named in `config_file`'s LogOnConfig, logs on, subscribes to
/// L1 quote+trade ticks for `symbols`, and returns a channel that yields each decoded tick
/// as it arrives. The session (heartbeats, TestRequest replies) runs in a background task
/// for as long as the returned receiver is alive.
pub async fn run(config_file: &str, symbols: Vec<String>) -> Result<Pin<Box<dyn Stream<Item = Result<Tick>> + Send + 'static>>> {
    let config = LogOnConfig::new(config_file)?;
    let stream = tcpsocket::connect(&config).await?;
    run_on_stream(stream, config, symbols).await
}

/// Same as [`run`], but against an already-connected transport. Kept generic over
/// `AsyncRead + AsyncWrite` (rather than the concrete TLS stream) so the session logic
/// can be driven by an in-memory stream in tests, with no real socket involved.
pub async fn run_on_stream<S>(
    stream: S,
    config: LogOnConfig,
    symbols: Vec<String>,
) -> Result<Pin<Box<dyn Stream<Item = Result<Tick>> + Send + 'static>>>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let (mut reader, mut writer) = split(stream);
    let heart_bt_int = config.heart_bt_int.max(1) as u64;

    let mut seq = 1u32;
    let mut cache = SymbolInitilizer::default();
    send(&mut writer, messages::logon(seq, &config, &sending_time())).await?;
    seq += 1;
    send(
        &mut writer,
        messages::market_data_request(seq, &config, &sending_time(), "mdr-1", &symbols),
    )
    .await?;

    let (tx, mut rx) = mpsc::channel(TICK_CHANNEL_CAPACITY);

    tokio::spawn(async move {
        let mut buf = BytesMut::new();
        let mut read_chunk = [0u8; READ_CHUNK_SIZE];
        let mut heartbeat_timer = interval(Duration::from_secs(heart_bt_int));
        heartbeat_timer.tick().await; // the first tick fires immediately; skip it

        loop {
            tokio::select! {
                read_result = reader.read(&mut read_chunk) => {
                    let n = match read_result {
                        Ok(0) | Err(_) => break, // connection closed
                        Ok(n) => n,
                    };
                    buf.extend_from_slice(&read_chunk[..n]);

                    while let Ok(Some(mut raw_bytes)) = framing::extract_message(&mut buf) {
                        let Ok(Some(raw_msg)) = FixCodec.decode(&mut raw_bytes) else {
                            continue; // malformed message: skip it, keep the session alive
                        };
                        if dispatch(raw_msg, &mut writer, &mut seq, &config, &tx, &mut cache).await.is_err() {
                            return;
                        }
                    }
                }
                _ = heartbeat_timer.tick() => {
                    seq += 1;
                    if send(&mut writer, messages::heartbeat(seq, &config, &sending_time(), None)).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    Ok(Box::pin(try_stream! {
        while let Some(tick) = rx.recv().await {
            yield tick
        }
    }))
}

/// Reacts to one decoded message: answers session-level control messages, and forwards
/// market-data ticks to the caller's channel. Returns `Err` only when the session should
/// stop (e.g. the exchange logged us out).
async fn dispatch<W: AsyncWrite + Unpin>(
    raw_msg: RawMessage,
    writer: &mut W,
    seq: &mut u32,
    config: &LogOnConfig,
    tx: &mpsc::Sender<Tick>,
    cache: &mut SymbolInitilizer,
) -> Result<()> {
    match raw_msg.get_tp() {
        Some(msg_type::TEST_REQUEST) => {
            let test_req_id = match raw_msg.get_val(tag::TEST_REQ_ID) {
                Some(ValType::Single(id)) => Some(id.clone()),
                _ => None,
            };
            *seq += 1;
            send(
                writer,
                messages::heartbeat(*seq, config, &sending_time(), test_req_id.as_deref()),
            )
            .await?;
        }
        Some(msg_type::MARKET_DATA_SNAPSHOT_FULL_REFRESH) | Some(msg_type::MARKET_DATA_INCREMENTAL_REFRESH) => {
            let Some(ValType::Single(symbol)) = raw_msg.get_val(tag::SYMBOL) else {
                return Ok(());
            };
            let symbol = cache.get_val(symbol);
            if let Ok(tick) = Tick::from_raw(raw_msg, symbol) {
                // Receiver dropped just means the caller is no longer listening; not our error to raise.
                let _ = tx.send(tick).await;
            }
        }
        Some(msg_type::RESEND_REQUEST) => {
            let start_seq = if let Some(val) = raw_msg.get_val(tag::BEGIN_SEQ) {
                match val {
                    ValType::Single(start_seq) => parse_uint(start_seq.as_bytes()),
                    _ => {return Err(anyhow!("Does not receive begin seq no on resend request"));},
                }
            } else {
                return Err(anyhow!("Receive invalid begin seq no on resend request"));
            };
            *seq += 1;
            send(
                writer,
                messages::seq_number_reset(start_seq, &config, &sending_time(), *seq)
            )
            .await?
        }
        Some(msg_type::LOGOUT) => return Err(anyhow!("exchange sent Logout, ending session")),
        _ => {}
    }
    Ok(())
}

async fn send<W: AsyncWrite + Unpin>(writer: &mut W, msg: RawMessage) -> Result<()> {
    let mut dst = BytesMut::new();
    FixCodec.encode(msg, &mut dst)?;
    writer.write_all(&dst).await?;
    Ok(())
}

fn sending_time() -> String {
    chrono::Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use futures::StreamExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
    use tokio_util::codec::{Decoder, Encoder};

    use crate::config::configs::LogOnConfig;
    use crate::fix::codec::FixCodec;
    use crate::fix::raw_msg::{RawMessage, ValType};
    use crate::fix::tag::{self, msg_type};
    use crate::network::framing;
    use crate::types::types::Tick;

    fn test_config() -> LogOnConfig {
        LogOnConfig::from_yaml(
            "\
host: exchange.example.com
port: \"9001\"
sender_id: US
target_id: EX
username: alice
password: secret
reset_seq_no: false
heart_bt_int: 3600
",
        )
        .unwrap()
    }

    /// Reads from `stream` until one full FIX message has arrived, then decodes it.
    /// Mirrors what the production read loop does, so the fake exchange in these
    /// tests exercises the same framing/codec path as the real session.
    async fn read_one_message<S: AsyncRead + Unpin>(stream: &mut S, buf: &mut BytesMut) -> RawMessage {
        loop {
            if let Some(mut raw_bytes) = framing::extract_message(buf).unwrap() {
                return FixCodec.decode(&mut raw_bytes).unwrap().unwrap();
            }
            let mut chunk = [0u8; 4096];
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0, "stream closed before a full message arrived");
            buf.extend_from_slice(&chunk[..n]);
        }
    }

    async fn write_message<S: tokio::io::AsyncWrite + Unpin>(stream: &mut S, msg: RawMessage) {
        let mut dst = BytesMut::new();
        FixCodec.encode(msg, &mut dst).unwrap();
        stream.write_all(&dst).await.unwrap();
    }

    fn logon_ack() -> RawMessage {
        let mut msg = RawMessage::new(4);
        msg.fields.push((tag::MSG_TYPE, ValType::Single(msg_type::LOGON.to_string())));
        msg.fields.push((tag::MSG_SEQ_NUM, ValType::Single("1".to_string())));
        msg
    }

    fn trade_tick(msg_seq_num: &str) -> RawMessage {
        let mut msg = RawMessage::new(8);
        msg.fields.push((
            tag::MSG_TYPE,
            ValType::Single(msg_type::MARKET_DATA_SNAPSHOT_FULL_REFRESH.to_string()),
        ));
        msg.fields.push((tag::MSG_SEQ_NUM, ValType::Single(msg_seq_num.to_string())));
        msg.fields.push((tag::SYMBOL, ValType::Single("BTC-USD".to_string())));
        msg.fields.push((tag::MD_ENTRY_NO, ValType::Single("1".to_string())));
        msg.fields.push((tag::MD_ENTRY_PX, ValType::Single("101.5".to_string())));
        msg.fields.push((tag::MD_ENTRY_SIZE, ValType::Single("10".to_string())));
        msg.fields.push((tag::MD_ENTRY_DATE, ValType::Single("20260826".to_string())));
        msg.fields.push((tag::MD_ENTRY_TIME, ValType::Single("12:00:00.000".to_string())));
        msg
    }

    fn test_request(test_req_id: &str) -> RawMessage {
        let mut msg = RawMessage::new(4);
        msg.fields.push((tag::MSG_TYPE, ValType::Single(msg_type::TEST_REQUEST.to_string())));
        msg.fields.push((tag::MSG_SEQ_NUM, ValType::Single("2".to_string())));
        msg.fields.push((tag::TEST_REQ_ID, ValType::Single(test_req_id.to_string())));
        msg
    }

    #[tokio::test]
    async fn logs_on_then_subscribes_before_forwarding_ticks() {
        let (client, mut exchange) = tokio::io::duplex(8192);
        let mut exchange_buf = BytesMut::new();

        let mut stream = run_on_stream(client, test_config(), vec!["BTC-USD".to_string()])
            .await
            .unwrap();

        let logon = read_one_message(&mut exchange, &mut exchange_buf).await;
        assert_eq!(logon.get_tp(), Some(msg_type::LOGON));
        write_message(&mut exchange, logon_ack()).await;

        let subscribe = read_one_message(&mut exchange, &mut exchange_buf).await;
        assert_eq!(subscribe.get_tp(), Some(msg_type::MARKET_DATA_REQUEST));

        write_message(&mut exchange, trade_tick("2")).await;

        let tick = stream.next().await.expect("tick channel closed early").unwrap();
        assert!(matches!(tick, Tick::Trade(_)));
    }

    #[tokio::test]
    async fn answers_test_request_with_a_heartbeat_echoing_its_id() {
        let (client, mut exchange) = tokio::io::duplex(8192);
        let mut exchange_buf = BytesMut::new();

        let _rx = run_on_stream(client, test_config(), vec!["BTC-USD".to_string()])
            .await
            .unwrap();

        read_one_message(&mut exchange, &mut exchange_buf).await; // Logon
        write_message(&mut exchange, logon_ack()).await;
        read_one_message(&mut exchange, &mut exchange_buf).await; // MarketDataRequest

        write_message(&mut exchange, test_request("probe-9")).await;

        let reply = read_one_message(&mut exchange, &mut exchange_buf).await;
        assert_eq!(reply.get_tp(), Some(msg_type::HEARTBEAT));
        match reply.get_val(tag::TEST_REQ_ID) {
            Some(ValType::Single(id)) => assert_eq!(id, "probe-9"),
            other => panic!("expected TestReqID to be echoed, got {other:?}"),
        }
    }
}
