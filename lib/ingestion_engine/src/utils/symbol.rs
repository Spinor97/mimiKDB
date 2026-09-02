use std::{collections::HashMap, sync::Arc};

#[derive(Default)]
pub struct SymbolInitilizer {
    cache: HashMap<String, Arc<str>>,
}

impl SymbolInitilizer {
    pub fn get_val(&mut self, symbol: &str) -> Arc<str> {
        if let Some(existing) = self.cache.get(symbol) {
            return existing.clone();
        }

        let val: Arc<str> = Arc::from(symbol);
        self.cache.insert(symbol.to_string(), val.clone());
        val
    }
}