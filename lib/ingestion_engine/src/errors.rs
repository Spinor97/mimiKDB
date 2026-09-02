use std::io;

#[derive(Debug)]
pub enum ParsingError {
    NotValidVal,
    NotCompleteSnapshot(i32),
    FailToLoad(io::Error),
}

impl From<io::Error> for ParsingError {
    fn from(value: io::Error) -> Self {
        ParsingError::FailToLoad(value)
    }
}

impl std::fmt::Display for ParsingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParsingError::NotValidVal => write!(f, "value is not valid"),
            ParsingError::NotCompleteSnapshot(remaining) => {
                write!(f, "incomplete snapshot, {remaining} entries still expected")
            }
            ParsingError::FailToLoad(source) => write!(f, "failed to load: {source}"),
        }
    }
}

impl std::error::Error for ParsingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ParsingError::FailToLoad(source) => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsing_error_converts_to_anyhow_with_readable_message() {
        let err: anyhow::Error = ParsingError::NotValidVal.into();
        assert_eq!(err.to_string(), "value is not valid");
    }
}