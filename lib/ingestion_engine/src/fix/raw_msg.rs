use std::io;

use crate::{errors::ParsingError, fix::{parser::FixField, tag}};

#[derive(Debug, Clone)]
pub enum ValType {
    Single(String),
    Pair((String, String)),
}

#[derive(Debug, Clone, Default)]
pub struct RawMessage {
    pub fields: Vec<(u32, ValType)>
}

impl RawMessage {
    pub fn new(cap: usize) -> Self {
        Self { fields: Vec::with_capacity(cap) }
    }
    
    pub fn get_val(&self, tag: u32) -> Option<&ValType> {
        self.fields
            .iter()
            .find(|(t, _)| *t == tag)
            .map(|(_, val)| val)
    }

    pub fn get_int(&self, tag: u32) -> Option<i32> {
        self.get_val(tag).and_then(|v| {
            match v {
                ValType::Single(val) => val.parse().ok(),
                _ => None,
            }
        })
    }

    pub fn get_float(&self, tag: u32) -> Option<f32> {
        self.get_val(tag).and_then(|v| {
            match v {
                ValType::Single(val) => val.parse().ok(),
                _ => None,
            }
        })
    }

    pub fn get_char(&self, tag: u32) -> Option<char> {
        self.get_val(tag).and_then(|v| {
            match v {
                ValType::Single(val) => val.chars().next(),
                _ => None,
            }
        })
    }

    pub fn get_tp(&self) -> Option<&str> {
        if let Some(ValType::Single(rtn)) = self.get_val(tag::MSG_TYPE) {
            return Some(rtn.as_str());
        } 

        None
    }

    pub fn get_pair_int(&self, tag: u32) -> (Option<i32>, Option<i32>) {
        if let Some(ValType::Pair((v1, v2))) = self.get_val(tag) {
            (v1.parse::<i32>().ok(), v2.parse::<i32>().ok())
        } else {
            (None, None)
        }
    }

    pub fn get_pair_float(&self, tag: u32) -> (Option<f32>, Option<f32>) {
        if let Some(ValType::Pair((v1, v2))) = self.get_val(tag) {
            (v1.parse::<f32>().ok(), v2.parse::<f32>().ok())
        } else {
            (None, None)
        }
    }

    #[inline(always)]
    pub fn append_single(&mut self, field: &FixField) -> Result<(), ParsingError> {
        self.fields.push((
            field.tag, 
            ValType::Single(
                str::from_utf8(field.value)
                    .map_err(|utf8_err| io::Error::new(io::ErrorKind::InvalidData, utf8_err))?
                    .to_string()
            )
        ));

        Ok(())
    }
}
