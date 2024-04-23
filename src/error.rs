use std::fmt;
use std::error::Error as StdError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    EmptyArguments,
    InvalidArguments(Vec<String>), // Contains invalid arguments but does not contain an underlying error
    DirectoryReadError(std::io::Error),
    CoreError,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::EmptyArguments => write!(f, "No arguments provided"),
            Error::InvalidArguments(ref args) => write!(f, "Invalid arguments provided: {:?}", args),
            Error::DirectoryReadError(ref e) => write!(f, "Error reading directory: {}", e),
            Error::CoreError => write!(f, "An error occurred in the core module"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match *self {
            Error::DirectoryReadError(ref e) => Some(e),
            _ => None,         }
    }
}

