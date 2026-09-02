#![allow(dead_code)]


use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::{errors::ParsingError, fix::{raw_msg::{RawMessage, ValType}, tag}, utils::times::combine_date_time};

#[derive(Clone, Debug)]
pub struct Quote {
    pub receive_time: DateTime<Utc>,
    pub ticker_name: Arc<str>,

    pub bid_price: f32,
    pub bid_vol: u32,

    pub ask_price: f32,
    pub ask_vol: u32,
}

#[derive(Debug, Clone)]
pub struct Trade {
    pub receive_time: DateTime<Utc>,
    pub ticker_name: Arc<str>,

    pub trade_px: f32,
    pub trade_vol: u32,
    pub trd_type: Option<u8>,
}

#[derive(Debug, Clone, Copy)]
pub enum Side {
    BUY = 0,
    SELL = 1,
}

impl Quote {
    pub fn from_raw(raw_msg: RawMessage, symbol: Arc<str>) -> Result<Self, ParsingError> {
        let Some(md_entry_no) = raw_msg.get_int(tag::MD_ENTRY_NO) else {
            return Err(ParsingError::NotValidVal);
        };

        if md_entry_no != 2 {
            return Err(ParsingError::NotCompleteSnapshot(md_entry_no));
        }

        let (Some(bid_px), Some(ask_px)) = raw_msg.get_pair_float(tag::MD_ENTRY_PX) else {
            return Err(ParsingError::NotValidVal);
        };

        let (Some(bid_vol), Some(ask_vol)) = raw_msg.get_pair_int(tag::MD_ENTRY_SIZE) else {
            return Err(ParsingError::NotValidVal);
        };

        let Some(ValType::Single(recv_date)) = raw_msg.get_val(tag::MD_ENTRY_DATE) else {
            return Err(ParsingError::NotValidVal);
        };

        let Some(ValType::Single(recv_time)) = raw_msg.get_val(tag::MD_ENTRY_TIME) else {
            return Err(ParsingError::NotValidVal);
        };

        let datetime = combine_date_time(recv_date, recv_time);

        Ok(Self {
            receive_time: datetime,
            ticker_name: symbol,
            bid_price: bid_px,
            bid_vol: bid_vol as u32,
            ask_price: ask_px,
            ask_vol: ask_vol as u32,
        })

    }
}

impl Trade {
    pub fn from_raw(raw_msg: RawMessage, symbol: Arc<str>) -> Result<Self, ParsingError> {

        let Some(trade_vol) = raw_msg.get_int(tag::MD_ENTRY_SIZE) else {
            return Err(ParsingError::NotValidVal);
        };

        let Some(trade_px) = raw_msg.get_float(tag::MD_ENTRY_PX) else {
            return Err(ParsingError::NotValidVal);
        };

        let Some(ValType::Single(recv_date)) = raw_msg.get_val(tag::MD_ENTRY_DATE) else {
            return Err(ParsingError::NotValidVal);
        };

        let Some(ValType::Single(recv_time)) = raw_msg.get_val(tag::MD_ENTRY_TIME) else {
            return Err(ParsingError::NotValidVal);
        };

        let datetime = combine_date_time(recv_date, recv_time);

        let trd_tp:Option<u8> = raw_msg.get_int(tag::TRADE_TYPE).map(|x| x as u8);

        Ok(Self {
            receive_time: datetime,
            ticker_name: symbol,
            trade_px: trade_px,
            trade_vol: trade_vol as u32,
            trd_type: trd_tp,
        })

    }
}

/// A single decoded market-data update, resolved from a raw FIX message into
/// whichever domain shape it actually is.
#[derive(Debug, Clone)]
pub enum Tick {
    Quote(Quote),
    Trade(Trade),
}

impl Tick {
    /// Mirrors `FixCodec::decode`'s own convention for MDEntryNo(268): a value
    /// of `1` means a single Trade entry, anything else means a Bid/Offer Quote pair.
    pub fn from_raw(raw_msg: RawMessage, symbol: Arc<str>) -> Result<Self, ParsingError> {
        match raw_msg.get_int(tag::MD_ENTRY_NO) {
            Some(1) => Trade::from_raw(raw_msg, symbol).map(Tick::Trade),
            _ => Quote::from_raw(raw_msg, symbol).map(Tick::Quote),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single_field(fields: &mut Vec<(u32, ValType)>, tag: u32, value: &str) {
        fields.push((tag, ValType::Single(value.to_string())));
    }

    #[test]
    fn resolves_to_trade_when_md_entry_no_is_one() {
        let mut raw = RawMessage::new(6);
        single_field(&mut raw.fields, tag::MD_ENTRY_NO, "1");
        single_field(&mut raw.fields, tag::MD_ENTRY_PX, "101.5");
        single_field(&mut raw.fields, tag::MD_ENTRY_SIZE, "10");
        single_field(&mut raw.fields, tag::MD_ENTRY_DATE, "20260826");
        single_field(&mut raw.fields, tag::MD_ENTRY_TIME, "12:00:00.000");

        let tick = Tick::from_raw(raw, Arc::from("DUMMY")).unwrap();

        assert!(matches!(tick, Tick::Trade(_)));
    }

    #[test]
    fn resolves_to_quote_when_md_entry_no_is_two() {
        let mut raw = RawMessage::new(6);
        single_field(&mut raw.fields, tag::MD_ENTRY_NO, "2");
        raw.fields.push((
            tag::MD_ENTRY_PX,
            ValType::Pair(("101.5".to_string(), "101.7".to_string())),
        ));
        raw.fields.push((
            tag::MD_ENTRY_SIZE,
            ValType::Pair(("10".to_string(), "20".to_string())),
        ));
        single_field(&mut raw.fields, tag::MD_ENTRY_DATE, "20260826");
        single_field(&mut raw.fields, tag::MD_ENTRY_TIME, "12:00:00.000");

        let tick = Tick::from_raw(raw, Arc::from("DUMMY")).unwrap();

        assert!(matches!(tick, Tick::Quote(_)));
    }

    #[test]
    fn propagates_the_underlying_error_when_shape_does_not_match() {
        let mut raw = RawMessage::new(2);
        single_field(&mut raw.fields, tag::MD_ENTRY_NO, "2");

        let result = Tick::from_raw(raw, Arc::from("DUMMY"));

        assert!(result.is_err());
    }
}
