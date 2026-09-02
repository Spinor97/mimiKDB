#![allow(dead_code)]

pub const BEGIN_STRING: u32 = 8;
pub const BODY_LENGTH: u32 = 9;
pub const CHECK_SUM: u32 = 10;

pub const ACCOUNT: u32 = 1;
pub const AVG_PX: u32 = 6;
pub const BEGIN_SEQ: u32 = 7;
pub const CL_ORD_ID: u32 = 11;
pub const CUM_QTY: u32 = 14;
pub const END_SEQ: u32 = 16;
pub const EXEC_ID: u32 = 17;
pub const HANDL_INST: u32 = 21;
pub const SECURITY_ID_SOURCE: u32 = 22; // legacy "IDSource"
pub const ORD_STATUS: u32 = 39;
pub const MSG_SEQ_NUM: u32 = 34;
pub const MSG_TYPE: u32 = 35;
pub const NEW_SEQ_NO: u32 = 36;
pub const ORDER_ID: u32 = 37;
pub const ORDER_QTY: u32 = 38;
pub const ORD_TYPE: u32 = 40;
pub const ORIG_CL_ORD_ID: u32 = 41;
pub const POSS_DUP_FLAG: u32 = 43;
pub const PRICE: u32 = 44;
pub const SECURITY_ID: u32 = 48;
pub const SENDER_COMP_ID: u32 = 49;
pub const SENDER_SUB_ID: u32 = 50;
pub const SENDING_TIME: u32 = 52;
pub const SIDE: u32 = 54;
pub const SYMBOL: u32 = 55;
pub const TARGET_COMP_ID: u32 = 56;
pub const TEXT: u32 = 58;
pub const TIME_IN_FORCE: u32 = 59;
pub const ENCRYPT_METHOD: u32 = 98;
pub const EX_DESTINATION: u32 = 100;
pub const HEART_BT_INT: u32 = 108;
pub const MIN_QTY: u32 = 110;
pub const MAX_FLOOR: u32 = 111;
pub const TEST_REQ_ID: u32 = 112;
pub const ORIG_SENDING_TIME: u32 = 122;
pub const GAP_FILL_FLAG: u32 = 123;
pub const RESET_SEQ_NUM_FLAG: u32 = 141;
pub const EXEC_TYPE: u32 = 150;
pub const LEAVES_QTY: u32 = 151;
pub const CXL_REJ_REASON: u32 = 102;
pub const SECURITY_EXCHANGE: u32 = 207;
pub const CXL_REJ_RESPONSE_TO: u32 = 434;
pub const NO_PARTY_IDS: u32 = 453;
pub const PARTY_ID: u32 = 448;
pub const PARTY_ROLE: u32 = 452;
pub const COMPLIANCE_ID: u32 = 376; // verify against your dictionary
pub const TARGET_STRATEGY: u32 = 847; // verify against your dictionary

// --- Confirmed custom/proprietary tags (from FIX44_PEL.xml in this folder) ---
pub const CITI_MAX_PCT_VOLUME: u32 = 7136; // "CitiMaxPctVolume"
pub const CUSTOM_OFFSET: u32 = 7146; // "Offset"

pub const MARKET_DEPTH: u32 = 264;

pub const MD_ENTRY_NO: u32 = 268;
pub const MD_ENTRY_TYPE: u32 = 269;
pub const MD_ENTRY_PX: u32 = 270;
pub const MD_ENTRY_SIZE: u32 = 271;
pub const MD_ENTRY_DATE: u32 = 272;
pub const MD_ENTRY_TIME: u32 = 273;

pub const TRADE_TYPE: u32 = 828;

pub const NO_RELATED_SYM: u32 = 146;
pub const MD_REQ_ID: u32 = 262;
pub const SUBSCRIPTION_REQUEST_TYPE: u32 = 263;
pub const NO_MD_ENTRY_TYPES: u32 = 267;
pub const USERNAME: u32 = 553;
pub const PASSWORD: u32 = 554;

/// MsgType(35) values relevant to order entry.
pub mod msg_type {
    pub const LOGON: &str = "A";
    pub const LOGOUT: &str = "5";
    pub const HEARTBEAT: &str = "0";
    pub const TEST_REQUEST: &str = "1";
    pub const RESEND_REQUEST: &str = "2";
    pub const REJECT: &str = "3";
    pub const SEQUENCE_RESET: &str = "4";
    pub const MARKET_DATA_REQUEST: &str = "V";
    pub const MARKET_DATA_SNAPSHOT_FULL_REFRESH: &str = "W";
    pub const MARKET_DATA_INCREMENTAL_REFRESH: &str = "X";
    pub const NEW_ORDER_SINGLE: &str = "D";
    pub const ORDER_CANCEL_REQUEST: &str = "F";
    pub const ORDER_CANCEL_REPLACE_REQUEST: &str = "G";
    pub const EXECUTION_REPORT: &str = "8";
    pub const ORDER_CANCEL_REJECT: &str = "9";
}
