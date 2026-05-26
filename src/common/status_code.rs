use s2n_quic::application::Error;

/// A simple enum which uses HTTP status codes
#[derive(Debug)]
pub enum StatusCode {
    OK = 200,
    InternalServerError = 500,
    ServiceUnavailable = 503,
}

impl From<StatusCode> for u32 {
    fn from(val: StatusCode) -> Self {
        val as u32
    }
}

impl From<StatusCode> for Error {
    fn from(value: StatusCode) -> Self {
        Self::new(value as u64).expect("Status code is exceeds the largest possible value allowed by a 62 bit unsigned integer")
    }
}
