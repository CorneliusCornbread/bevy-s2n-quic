//! Sends a connection's datagrams and counts the ones quinn drops.
//!
//! `Datagrams::send(data, true)` drops the oldest queued datagrams when the
//! send buffer is full, but doesn't say how many. [`DatagramQueue`] mirrors
//! quinn's queue (the length of each datagram in it) and applies the same
//! rule, so the count is exact.

use std::collections::VecDeque;

use bytes::Bytes;
use quinn_proto::Connection;

/// The smallest path MTU QUIC allows, which quinn falls back to when it
/// detects a black hole.
const QUIC_MIN_MTU: u16 = 1200;

pub(super) struct DatagramQueue {
    /// Payload lengths of the datagrams quinn has queued, oldest first.
    queued: VecDeque<usize>,
    payload: usize,
    /// The size of quinn's send buffer.
    capacity: usize,
    /// What quinn counts per queued datagram on top of its payload.
    overhead: usize,
    /// quinn's count of datagram frames sent, when the queue was last synced.
    sent: u64,
    dropped: u64,
}

impl DatagramQueue {
    /// Creates the mirror for a new connection, whose queue is still empty.
    /// `capacity` must be the connection's datagram send buffer size.
    pub fn new(conn: &mut Connection, capacity: usize) -> Self {
        let space = conn.datagrams().send_buffer_space();
        Self {
            queued: VecDeque::new(),
            payload: 0,
            capacity,
            overhead: capacity.saturating_sub(space),
            sent: conn.stats().frame_tx.datagram,
            dropped: 0,
        }
    }

    /// Datagrams that were queued but will never be sent: dropped to make
    /// room, too large for the path, or sent before the peer accepted any.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Queues `datagrams` on quinn, dropping the oldest queued ones when
    /// there's no room.
    pub fn send(&mut self, conn: &mut Connection, datagrams: Vec<Bytes>) {
        self.sync(conn);

        for data in datagrams {
            let len = data.len();
            if let Err(e) = conn.datagrams().send(data, true) {
                tracing::trace!("dropping a datagram: {e}");
                self.dropped += 1;
                continue;
            }

            // quinn's rule: drop the oldest until the new one fits.
            while !self.fits(len) {
                let Some(old) = self.queued.pop_front() else {
                    break;
                };
                self.payload -= old;
                self.dropped += 1;
            }
            self.queued.push_back(len);
            self.payload += len;
        }
    }

    /// Forgets the datagrams quinn sent since the last sync, then checks the
    /// mirror still matches quinn's queue.
    fn sync(&mut self, conn: &mut Connection) {
        let sent = conn.stats().frame_tx.datagram;
        // Datagrams are always sent oldest first.
        for _ in 0..sent.saturating_sub(self.sent) {
            let Some(len) = self.queued.pop_front() else {
                break;
            };
            self.payload -= len;
        }
        self.sent = sent;

        if self.matches(conn) {
            return;
        }

        // quinn drops queued datagrams that no longer fit when the path MTU
        // shrinks.
        if let Some(max) = conn.datagrams().max_size() {
            let before = self.queued.len();
            self.queued.retain(|&len| len <= max);
            self.dropped += (before - self.queued.len()) as u64;
            self.payload = self.queued.iter().sum();
        }
        if !self.matches(conn) {
            tracing::debug!("lost track of the queued datagrams; drop counts may be low");
            self.queued.clear();
            self.payload = 0;
        }
    }

    fn memory(&self) -> usize {
        self.payload + self.queued.len() * self.overhead
    }

    fn fits(&self, len: usize) -> bool {
        self.memory() + len + self.overhead <= self.capacity
    }

    fn matches(&self, conn: &mut Connection) -> bool {
        let space = self
            .capacity
            .saturating_sub(self.memory())
            .saturating_sub(self.overhead);
        space == conn.datagrams().send_buffer_space()
    }
}

/// The largest datagram the connection can send now, and the largest it can
/// always send, at QUIC's minimum path MTU. Both are `None` if the peer
/// doesn't accept datagrams.
pub(super) fn datagram_sizes(conn: &mut Connection) -> (Option<usize>, Option<usize>) {
    let max = conn.datagrams().max_size();
    // The size shrinks byte for byte with the MTU. If the peer's limit is
    // lower than the path's, this underestimates, which is safe.
    let above_min = conn.current_mtu().saturating_sub(QUIC_MIN_MTU) as usize;
    (max, max.map(|max| max.saturating_sub(above_min)))
}
