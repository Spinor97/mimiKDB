use bytes::BytesMut;

use crate::{errors::ParsingError, fix::{parser::FixParser, tag}};

/// Trailer length for `10=` + a fixed 3-digit checksum + the SOH delimiter,
/// matching how `FixCodec::encode` always writes the checksum as `{checksum:03}`.
const TRAILER_LEN: usize = 7;

/// Pulls exactly one complete FIX message out of `buf`, if one is fully present.
///
/// Looks at the BeginString(8)/BodyLength(9) header to compute the full message
/// length, then only removes those bytes from `buf` once they have all arrived -
/// leaving a partial message untouched for the next call. This lets a raw byte
/// stream (which may deliver partial messages or several concatenated ones per
/// read) be split into individual messages before handing each one to
/// `FixCodec::decode`.
pub fn extract_message(buf: &mut BytesMut) -> Result<Option<BytesMut>, ParsingError> {
    let mut parser = FixParser::new(buf.as_ref());

    let Some(begin_string) = parser.next_field() else {
        return Ok(None);
    };
    if begin_string.tag != tag::BEGIN_STRING {
        return Err(ParsingError::NotValidVal);
    }

    let Some(body_length) = parser.next_field() else {
        return Ok(None);
    };
    if body_length.tag != tag::BODY_LENGTH {
        return Err(ParsingError::NotValidVal);
    }

    let body_len: usize = str::from_utf8(body_length.value)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(ParsingError::NotValidVal)?;

    let total_len = parser.pos() + body_len + TRAILER_LEN;

    if buf.len() < total_len {
        return Ok(None);
    }

    Ok(Some(buf.split_to(total_len)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn fix_message(body: &str) -> String {
        let with_type = format!("35=A\x01{body}");
        format!("8=FIX.4.4\x019={}\x01{with_type}10=000\x01", with_type.len())
    }

    #[test]
    fn returns_none_on_empty_buffer() {
        let mut buf = BytesMut::new();

        let result = extract_message(&mut buf).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn returns_none_while_header_is_still_incomplete() {
        let mut buf = BytesMut::from(&b"8=FIX.4.4\x019=1"[..]);

        let result = extract_message(&mut buf).unwrap();

        assert!(result.is_none());
        assert_eq!(buf.len(), 13, "incomplete data must not be consumed");
    }

    #[test]
    fn returns_none_while_body_is_still_arriving() {
        let full = fix_message("49=A\x01");
        let mut buf = BytesMut::from(&full.as_bytes()[..full.len() - 5]);

        let result = extract_message(&mut buf).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn extracts_one_complete_message_and_drains_it_from_the_buffer() {
        let full = fix_message("49=A\x01");
        let mut buf = BytesMut::from(full.as_bytes());

        let extracted = extract_message(&mut buf).unwrap().unwrap();

        assert_eq!(extracted.as_ref(), full.as_bytes());
        assert!(buf.is_empty());
    }

    #[test]
    fn extracts_first_message_and_leaves_the_rest_for_the_next_call() {
        let first = fix_message("49=A\x01");
        let second = fix_message("49=B\x01");
        let mut buf = BytesMut::from(format!("{first}{second}").as_bytes());

        let extracted = extract_message(&mut buf).unwrap().unwrap();

        assert_eq!(extracted.as_ref(), first.as_bytes());
        assert_eq!(buf.as_ref(), second.as_bytes());

        let extracted_second = extract_message(&mut buf).unwrap().unwrap();
        assert_eq!(extracted_second.as_ref(), second.as_bytes());
        assert!(buf.is_empty());
    }
}
