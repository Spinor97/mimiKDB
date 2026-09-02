
pub const SOH: u8 = 0x01;

pub const EQUALS: u8 = b'=';

#[derive(Debug, Clone, Copy)]
pub struct FixField<'a> {
    pub tag: u32,
    pub value: &'a [u8],
}

pub struct FixParser<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> FixParser<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            data: buf,
            pos: 0
        }
    }

    /// Byte offset into the input buffer just past the last field returned by `next_field`.
    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn next_field(&mut self) -> Option<FixField<'a>> {
        if self.pos >= self.data.len() {
            return None;
        }
        
        let mut tag: u32 = 0;
        while self.pos < self.data.len() && self.data[self.pos] != EQUALS {
            let byte = self.data[self.pos];
            // A tag is only ever ASCII digits before '='. Anything else
            // (arbitrary bytes from an untrusted peer, or -- now that this
            // parser is reused for query_engine's protocol -- a client
            // sending something that isn't this wire format at all) means
            // this isn't a well-formed field: `byte - b'0'` would underflow
            // for any byte below '0', so stop here instead of panicking.
            if !byte.is_ascii_digit() {
                return None;
            }
            // checked, not `tag * 10 + ...`: an unreasonably long digit run
            // (also untrusted-input-controlled) would overflow u32 the same
            // way the underflow above did -- same fix, same reason.
            tag = tag.checked_mul(10)?.checked_add((byte - b'0') as u32)?;
            self.pos += 1;
        }

        if self.pos >= self.data.len() {
            return None; // Malformed: no '='
        }

        self.pos += 1; // Skip '='
        // Parse value (bytes before SOH)
        let value_start = self.pos;
        while self.pos < self.data.len() && self.data[self.pos] != SOH {
            self.pos += 1;
        }

        let value = &self.data[value_start..self.pos];
        if self.pos < self.data.len() {
            self.pos += 1; // Skip SOH
        }

        Some(FixField {
            tag,
            value
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_well_formed_tag_value_pairs() {
        let mut parser = FixParser::new(b"8=FIX.4.2\x0135=A\x01");

        let f1 = parser.next_field().unwrap();
        assert_eq!(f1.tag, 8);
        assert_eq!(f1.value, b"FIX.4.2");

        let f2 = parser.next_field().unwrap();
        assert_eq!(f2.tag, 35);
        assert_eq!(f2.value, b"A");

        assert!(parser.next_field().is_none());
    }

    #[test]
    fn returns_none_instead_of_panicking_on_a_non_digit_tag() {
        // Regression test: the tag-accumulation loop used to do
        // `data[pos] - b'0'` unconditionally, which underflows (panics in
        // debug builds) for any byte below '0' -- e.g. the space in plain
        // text that was never meant to be parsed as this wire format.
        let mut parser = FixParser::new(b"SELECT * FROM trades");
        assert!(parser.next_field().is_none());
    }

    #[test]
    fn returns_none_instead_of_overflowing_on_an_unreasonably_long_tag() {
        let too_many_digits = "9".repeat(20) + "=x\x01";
        let mut parser = FixParser::new(too_many_digits.as_bytes());
        assert!(parser.next_field().is_none());
    }
}