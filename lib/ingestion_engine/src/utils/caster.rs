#[inline(always)]
pub fn parse_uint(value: &[u8]) -> u32 {
    let mut result: u32 = 0;
    for &b in value {
        result = result * 10 + (b - b'0') as u32;
    }
    result
}