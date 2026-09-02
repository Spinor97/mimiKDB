use std::io;

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::{errors::ParsingError, fix::{parser::{FixParser, SOH}, raw_msg::{RawMessage, ValType}, tag}};

const FIX_VERSION: &'static str = "FIX.4.4";

fn parse_quote(parser: &mut FixParser, raw_msg: &mut RawMessage) -> Result<(), ParsingError>{
    let mut curr_side = 0;
    let mut px_pair = ("".to_string(), "".to_string());
    let mut sz_pair = ("".to_string(), "".to_string());

    while let Some(field) = parser.next_field() {

        match field.tag {
            tag::MD_ENTRY_NO => {
                curr_side = tag::MD_ENTRY_NO;
                raw_msg.append_single(&field)?;
            },
            tag::MD_ENTRY_DATE => {
                raw_msg.append_single(&field)?;
            },
            tag::MD_ENTRY_TIME => {
                raw_msg.append_single(&field)?;
            },
            tag::MD_ENTRY_TYPE => {
                raw_msg.append_single(&field)?;
            },
            tag::MD_ENTRY_PX => {
                if curr_side == 0 {
                    px_pair.0 = str::from_utf8(field.value)
                            .map_err(|utf8_err| io::Error::new(io::ErrorKind::InvalidData, utf8_err))?
                            .to_string();
                } else {
                    px_pair.1 = str::from_utf8(field.value)
                            .map_err(|utf8_err| io::Error::new(io::ErrorKind::InvalidData, utf8_err))?
                            .to_string();
                }
            },
            tag::MD_ENTRY_SIZE => {
                if curr_side == 0 {
                    sz_pair.0 = str::from_utf8(field.value)
                            .map_err(|utf8_err| io::Error::new(io::ErrorKind::InvalidData, utf8_err))?
                            .to_string();
                } else {
                    sz_pair.1 = str::from_utf8(field.value)
                            .map_err(|utf8_err| io::Error::new(io::ErrorKind::InvalidData, utf8_err))?
                            .to_string();
                }
            },
            tag::CHECK_SUM => {
                // Trailing checksum of a wire-encoded message; not a repeating-group entry field.
            },
            _ => {return Err(ParsingError::FailToLoad(io::Error::new(io::ErrorKind::InvalidData, "Unexpected or unknown tag encountered")));}
        }
    }


    raw_msg.fields.push((tag::MD_ENTRY_PX, ValType::Pair(px_pair)));
    raw_msg.fields.push((tag::MD_ENTRY_SIZE, ValType::Pair(sz_pair)));
    Ok(())
}

fn parse_trade(parser: &mut FixParser, raw_msg: &mut RawMessage) -> Result<(), ParsingError> {
    while let Some(field) = parser.next_field() {

        match field.tag {
            tag::MD_ENTRY_NO => {
                raw_msg.append_single(&field)?;
            },
            tag::MD_ENTRY_DATE => {
                raw_msg.append_single(&field)?;
            },
            tag::MD_ENTRY_TIME => {
                raw_msg.append_single(&field)?;
            },
            tag::MD_ENTRY_TYPE => {
                raw_msg.append_single(&field)?;
            },
            tag::MD_ENTRY_SIZE => {
                raw_msg.append_single(&field)?;
            },
            tag::MD_ENTRY_PX => {
                raw_msg.append_single(&field)?;
            },
            tag::CHECK_SUM => {
                // Trailing checksum of a wire-encoded message; not a repeating-group entry field.
            },
            _ => {return Err(ParsingError::FailToLoad(io::Error::new(io::ErrorKind::InvalidData, "Unexpected or unknown tag encountered")));}
        }
    }

    Ok(())
}

pub struct FixCodec;

impl Decoder for FixCodec {
    type Item = RawMessage;
    
    type Error = ParsingError;
    
    fn decode<'a>(&mut self, src: &'a mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut rtn = RawMessage::new(10);

        let mut fix_parser = FixParser::new(src.as_ref());
        let mut is_quote = true;

        while let Some(field) = fix_parser.next_field() {
            rtn.append_single(&field)?;
            if field.tag == tag::MD_ENTRY_NO {
                match parse_uint(field.value) {
                    1 => {is_quote = false;},
                    _ => {},
                }

                break;
            }
        }

        if is_quote {
            parse_quote(&mut fix_parser, &mut rtn)?;
        } else {
            parse_trade(&mut fix_parser, &mut rtn)?;
        }

        Ok(Some(rtn))
    }


}

impl Encoder<RawMessage> for FixCodec {
    type Error = ParsingError;

    fn encode(&mut self, msg: RawMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut body = BytesMut::new();
        for (tag, value) in &msg.fields {
            if *tag == tag::BEGIN_STRING || *tag == tag::BODY_LENGTH || *tag == tag::CHECK_SUM {
                continue;
            }
            body.extend_from_slice(tag.to_string().as_bytes());
            body.extend_from_slice(b"=");
            if let ValType::Single(val) = value {
                body.extend_from_slice(val.as_bytes());
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Should not have pair val in the current version, check the response logic!").into());
            }
            
            body.extend_from_slice(&[SOH]);
        }

        let mut full = BytesMut::new();
        full.extend_from_slice(format!("8={FIX_VERSION}").as_bytes());
        full.extend_from_slice(&[SOH]);
        full.extend_from_slice(format!("9={}", body.len()).as_bytes());
        full.extend_from_slice(&[SOH]);
        full.extend_from_slice(&body);

        let checksum: u32 = full.iter().map(|&b| b as u32).sum::<u32>() & 255;

        dst.extend_from_slice(&full);
        dst.extend_from_slice(format!("10={checksum:03}").as_bytes());
        dst.extend_from_slice(&[SOH]);
        Ok(())
    }
}

fn parse_uint(buf: &[u8]) -> u32 {
    let rtn = 0;
    buf.iter()
        .fold(
            rtn,
            |mut rtn, &b| {
                rtn *= 10;
                rtn += (b - b'0') as u32;
                rtn
            }
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A message encoded by `FixCodec` (as it will actually appear on the wire, complete
    /// with the trailing checksum field) must decode back successfully - not just the
    /// body fragment with the trailer stripped off.
    #[test]
    fn decodes_a_wire_encoded_trade_message_including_its_checksum_trailer() {
        let mut trade = RawMessage::new(6);
        trade.fields.push((tag::MSG_TYPE, ValType::Single("W".to_string())));
        trade.fields.push((tag::MSG_SEQ_NUM, ValType::Single("2".to_string())));
        trade.fields.push((tag::MD_ENTRY_NO, ValType::Single("1".to_string())));
        trade.fields.push((tag::MD_ENTRY_PX, ValType::Single("101.5".to_string())));
        trade.fields.push((tag::MD_ENTRY_SIZE, ValType::Single("10".to_string())));
        trade.fields.push((tag::MD_ENTRY_DATE, ValType::Single("20260826".to_string())));
        trade.fields.push((tag::MD_ENTRY_TIME, ValType::Single("12:00:00.000".to_string())));

        let mut wire = BytesMut::new();
        FixCodec.encode(trade, &mut wire).unwrap();

        let decoded = FixCodec.decode(&mut wire).unwrap().unwrap();

        assert_eq!(decoded.get_int(tag::MD_ENTRY_SIZE), Some(10));
    }
}