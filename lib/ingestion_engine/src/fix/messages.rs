use crate::config::configs::LogOnConfig;
use crate::fix::raw_msg::{RawMessage, ValType};
use crate::fix::tag::{self, msg_type};

/// L1 depth: top-of-book Bid/Offer plus Trade prints.
const L1_ENTRY_TYPES: [&str; 3] = ["0", "1", "2"];

fn header(msg_type: &str, seq: u32, config: &LogOnConfig, sending_time: &str) -> RawMessage {
    let mut msg = RawMessage::new(8);
    msg.fields.push((tag::MSG_TYPE, ValType::Single(msg_type.to_string())));
    msg.fields.push((tag::MSG_SEQ_NUM, ValType::Single(seq.to_string())));
    msg.fields.push((tag::SENDER_COMP_ID, ValType::Single(config.sender_id.clone())));
    msg.fields.push((tag::TARGET_COMP_ID, ValType::Single(config.target_id.clone())));
    msg.fields.push((tag::SENDING_TIME, ValType::Single(sending_time.to_string())));
    msg
}

/// Builds a Logon(A) message carrying credentials and session settings from `config`.
pub fn logon(seq: u32, config: &LogOnConfig, sending_time: &str) -> RawMessage {
    let mut msg = header(msg_type::LOGON, seq, config, sending_time);
    msg.fields.push((tag::ENCRYPT_METHOD, ValType::Single("0".to_string())));
    msg.fields.push((tag::HEART_BT_INT, ValType::Single(config.heart_bt_int.to_string())));
    let reset_flag = if config.reset_seq_no { "Y" } else { "N" };
    msg.fields.push((tag::RESET_SEQ_NUM_FLAG, ValType::Single(reset_flag.to_string())));
    msg.fields.push((tag::USERNAME, ValType::Single(config.username.clone())));
    msg.fields.push((tag::PASSWORD, ValType::Single(config.password.clone())));
    msg
}

/// Builds a Heartbeat(0). Pass the incoming TestReqID(112) to answer a TestRequest(1);
/// pass `None` for a heartbeat sent on the regular interval.
pub fn heartbeat(seq: u32, config: &LogOnConfig, sending_time: &str, test_req_id: Option<&str>) -> RawMessage {
    let mut msg = header(msg_type::HEARTBEAT, seq, config, sending_time);
    if let Some(id) = test_req_id {
        msg.fields.push((tag::TEST_REQ_ID, ValType::Single(id.to_string())));
    }
    msg
}

/// Builds a MarketDataRequest(V) subscribing (snapshot + updates) to L1 quote and
/// trade ticks for every symbol in `symbols`.
pub fn market_data_request(
    seq: u32,
    config: &LogOnConfig,
    sending_time: &str,
    req_id: &str,
    symbols: &[String],
) -> RawMessage {
    let mut msg = header(msg_type::MARKET_DATA_REQUEST, seq, config, sending_time);
    msg.fields.push((tag::MD_REQ_ID, ValType::Single(req_id.to_string())));
    msg.fields.push((tag::SUBSCRIPTION_REQUEST_TYPE, ValType::Single("1".to_string())));
    msg.fields.push((tag::MARKET_DEPTH, ValType::Single("1".to_string())));

    msg.fields.push((tag::NO_MD_ENTRY_TYPES, ValType::Single(L1_ENTRY_TYPES.len().to_string())));
    for entry_type in L1_ENTRY_TYPES {
        msg.fields.push((tag::MD_ENTRY_TYPE, ValType::Single(entry_type.to_string())));
    }

    msg.fields.push((tag::NO_RELATED_SYM, ValType::Single(symbols.len().to_string())));
    for symbol in symbols {
        msg.fields.push((tag::SYMBOL, ValType::Single(symbol.clone())));
    }

    msg
}

pub fn seq_number_reset(seq: u32, config: &LogOnConfig, sending_time: &str, new_start: u32) -> RawMessage {
    let mut msg = header(msg_type::SEQUENCE_RESET, seq, config, sending_time);
    msg.fields.push((tag::GAP_FILL_FLAG, ValType::Single("Y".to_string())));
    msg.fields.push((tag::POSS_DUP_FLAG, ValType::Single("Y".to_string())));

    msg.fields.push((tag::NEW_SEQ_NO, ValType::Single(new_start.to_string())));

    msg
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::configs::LogOnConfig;
    use crate::fix::raw_msg::ValType;

    fn test_config() -> LogOnConfig {
        LogOnConfig::from_yaml(
            "\
host: exchange.example.com
port: \"9001\"
sender_id: US
target_id: EX
username: alice
password: secret
reset_seq_no: true
heart_bt_int: 30
",
        )
        .unwrap()
    }

    fn single(msg: &RawMessage, t: u32) -> &str {
        match msg.get_val(t).expect("tag missing") {
            ValType::Single(v) => v.as_str(),
            ValType::Pair(_) => panic!("expected a single value"),
        }
    }

    fn all_singles(msg: &RawMessage, t: u32) -> Vec<&str> {
        msg.fields
            .iter()
            .filter(|(tag, _)| *tag == t)
            .map(|(_, v)| match v {
                ValType::Single(v) => v.as_str(),
                ValType::Pair(_) => panic!("expected a single value"),
            })
            .collect()
    }

    #[test]
    fn logon_carries_credentials_and_session_settings() {
        let config = test_config();

        let msg = logon(1, &config, "20260826-12:00:00.000");

        assert_eq!(single(&msg, tag::MSG_TYPE), msg_type::LOGON);
        assert_eq!(single(&msg, tag::MSG_SEQ_NUM), "1");
        assert_eq!(single(&msg, tag::SENDER_COMP_ID), "US");
        assert_eq!(single(&msg, tag::TARGET_COMP_ID), "EX");
        assert_eq!(single(&msg, tag::USERNAME), "alice");
        assert_eq!(single(&msg, tag::PASSWORD), "secret");
        assert_eq!(single(&msg, tag::HEART_BT_INT), "30");
        assert_eq!(single(&msg, tag::RESET_SEQ_NUM_FLAG), "Y");
    }

    #[test]
    fn heartbeat_without_test_req_id_omits_the_tag() {
        let config = test_config();

        let msg = heartbeat(2, &config, "20260826-12:00:00.000", None);

        assert_eq!(single(&msg, tag::MSG_TYPE), msg_type::HEARTBEAT);
        assert!(msg.get_val(tag::TEST_REQ_ID).is_none());
    }

    #[test]
    fn heartbeat_replying_to_a_test_request_echoes_its_id() {
        let config = test_config();

        let msg = heartbeat(3, &config, "20260826-12:00:00.000", Some("probe-1"));

        assert_eq!(single(&msg, tag::MSG_TYPE), msg_type::HEARTBEAT);
        assert_eq!(single(&msg, tag::TEST_REQ_ID), "probe-1");
    }

    #[test]
    fn market_data_request_subscribes_to_l1_quote_and_trade_for_every_symbol() {
        let config = test_config();
        let symbols = vec!["BTC-USD".to_string(), "ETH-USD".to_string()];

        let msg = market_data_request(4, &config, "20260826-12:00:00.000", "req-1", &symbols);

        assert_eq!(single(&msg, tag::MSG_TYPE), msg_type::MARKET_DATA_REQUEST);
        assert_eq!(single(&msg, tag::MD_REQ_ID), "req-1");
        assert_eq!(single(&msg, tag::SUBSCRIPTION_REQUEST_TYPE), "1");
        assert_eq!(single(&msg, tag::MARKET_DEPTH), "1");
        assert_eq!(single(&msg, tag::NO_MD_ENTRY_TYPES), "3");
        assert_eq!(all_singles(&msg, tag::MD_ENTRY_TYPE), vec!["0", "1", "2"]);
        assert_eq!(single(&msg, tag::NO_RELATED_SYM), "2");
        assert_eq!(all_singles(&msg, tag::SYMBOL), vec!["BTC-USD", "ETH-USD"]);
    }
}
