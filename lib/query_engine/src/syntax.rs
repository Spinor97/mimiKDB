pub(crate) const QUERY_TYPE: u32 = 0;
pub(crate) const SYMBOL: u32 = 1;
pub(crate) const SENTENCE: u32 = 2;
pub(crate) const STT_DATE: u32 = 3;
pub(crate) const EDD_DATE: u32 = 4;

pub(crate) mod query_type {
    pub(crate) const SQL_CMD: &str = "0";
    pub(crate) const JOIN_AS_OF: &str = "1";
}