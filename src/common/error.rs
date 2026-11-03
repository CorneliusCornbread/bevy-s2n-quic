use std::fmt;

use bytes::Bytes;
use thiserror::Error;

use crate::common::status_code::StatusCode;

/// An application-defined error code, sent to the peer when closing a
/// connection or stopping or resetting a stream.
///
/// QUIC encodes these as variable-length integers, so a code is at most
/// [`AppErrorCode::MAX`] (2⁶² − 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AppErrorCode(u64);

impl AppErrorCode {
    /// The largest code QUIC can encode, 2⁶² − 1.
    pub const MAX: u64 = (1 << 62) - 1;

    /// Code `0`, used when the application has no specific code to send.
    pub const UNKNOWN: Self = Self(0);

    /// Creates a code, or returns `None` if `code` is larger than [`Self::MAX`].
    pub const fn new(code: u64) -> Option<Self> {
        if code > Self::MAX {
            None
        } else {
            Some(Self(code))
        }
    }

    /// Creates a code from a `u32`, which always fits.
    pub const fn from_u32(code: u32) -> Self {
        Self(code as u64)
    }

    /// The numeric value of this code.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for AppErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u8> for AppErrorCode {
    fn from(code: u8) -> Self {
        Self(code.into())
    }
}

impl From<u16> for AppErrorCode {
    fn from(code: u16) -> Self {
        Self(code.into())
    }
}

impl From<u32> for AppErrorCode {
    fn from(code: u32) -> Self {
        Self(code.into())
    }
}

impl From<StatusCode> for AppErrorCode {
    fn from(code: StatusCode) -> Self {
        Self::from(u32::from(code))
    }
}

impl From<AppErrorCode> for u64 {
    fn from(code: AppErrorCode) -> Self {
        code.0
    }
}

/// Why a connection was closed or could not be established.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ConnectionError {
    /// The connection was idle, or the handshake took, longer than the timeout.
    #[error("the connection timed out")]
    TimedOut,
    /// The peer reset the connection without closing it.
    #[error("the connection was reset by the peer")]
    Reset,
    /// The peer's application closed the connection.
    #[error("the peer closed the connection with code {code}")]
    ApplicationClosed {
        /// The code the peer sent.
        code: AppErrorCode,
        /// The reason the peer sent, possibly empty.
        reason: Bytes,
    },
    /// The peer's QUIC stack closed the connection.
    #[error("the peer closed the connection with transport code {code}")]
    ConnectionClosed {
        /// The QUIC transport error code. `0` means there was no error.
        code: u64,
        /// The reason the peer sent, possibly empty.
        reason: Bytes,
    },
    /// This side closed the connection.
    #[error("the connection was closed locally")]
    LocallyClosed,
    /// The peer doesn't support any QUIC version this side offered.
    #[error("the peer doesn't support a compatible QUIC version")]
    VersionMismatch,
    /// Any other transport-level failure.
    #[error("transport error: {0}")]
    Transport(String),
}

/// Why data could not be queued on a send stream. Holds the data that wasn't
/// queued.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SendError<T> {
    /// The send buffer is full. Try again later.
    #[error("the send buffer is full")]
    Full(T),
    /// The stream is closed or closing, so it no longer accepts data.
    #[error("the stream is closed")]
    Closed(T),
}

impl<T> SendError<T> {
    /// Returns the data that wasn't queued.
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(data) | Self::Closed(data) => data,
        }
    }
}

/// Why a datagram could not be queued with
/// [QuicConnection::send_datagram][crate::common::connection::QuicConnection::send_datagram].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DatagramError {
    /// The connection is closed.
    #[error("the connection is closed")]
    Closed,
    /// The peer doesn't accept datagrams.
    #[error("the peer doesn't accept datagrams")]
    Unsupported,
    /// The datagram is larger than the path currently allows.
    #[error("the datagram is {size} bytes, but at most {max} fit")]
    TooLarge {
        /// The datagram's size.
        size: usize,
        /// The largest datagram that fits.
        max: usize,
    },
}
