//! Length-prefixed framing for aeronet packets sent on a stream.
//!
//! A stream delivers bytes, not packets: data can arrive split or merged at
//! any point. Each packet is therefore sent as a big-endian `u16` length
//! followed by the packet, and the receiver puts the packets back together.

use bevy::ecs::component::Component;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// The largest packet a frame can hold.
pub(crate) const MAX_PACKET: usize = u16::MAX as usize;

const PREFIX: usize = 2;

/// Packets framed into one buffer by [encode].
pub(crate) struct Encoded {
    pub data: Bytes,
    pub packets: usize,
    /// Bytes of the packets themselves, not counting the prefixes.
    pub payload: usize,
    /// Packets skipped because they're larger than [MAX_PACKET].
    pub oversized: usize,
}

/// Frames `packets`, in order, into one buffer.
pub(crate) fn encode(packets: &[Bytes]) -> Encoded {
    let (fit, oversized): (Vec<_>, Vec<_>) =
        packets.iter().partition(|packet| packet.len() <= MAX_PACKET);
    let payload: usize = fit.iter().map(|packet| packet.len()).sum();

    let mut data = BytesMut::with_capacity(payload + fit.len() * PREFIX);
    for packet in &fit {
        data.put_u16(packet.len() as u16);
        data.put_slice(packet);
    }

    Encoded {
        data: data.freeze(),
        packets: fit.len(),
        payload,
        oversized: oversized.len(),
    }
}

/// Puts packets back together from a stream's data.
#[derive(Component, Default)]
pub(crate) struct FrameDecoder {
    /// The start of a packet split across chunks.
    partial: BytesMut,
}

impl FrameDecoder {
    /// Takes the next chunk of the stream, calling `emit` with each packet
    /// it completes. Whole packets in `chunk` aren't copied.
    pub fn push(&mut self, mut chunk: Bytes, mut emit: impl FnMut(Bytes)) {
        while !self.partial.is_empty() {
            if self.partial.len() < PREFIX {
                self.fill(&mut chunk, PREFIX);
                if self.partial.len() < PREFIX {
                    return;
                }
            }

            let want = PREFIX + packet_len(&self.partial);
            self.fill(&mut chunk, want);
            if self.partial.len() < want {
                return;
            }
            self.partial.advance(PREFIX);
            emit(self.partial.split().freeze());
        }

        while chunk.len() >= PREFIX {
            let len = packet_len(&chunk);
            if chunk.len() < PREFIX + len {
                break;
            }
            chunk.advance(PREFIX);
            emit(chunk.split_to(len));
        }
        self.partial.extend_from_slice(&chunk);
    }

    /// Moves bytes from `chunk` until `partial` holds `want` bytes or
    /// `chunk` is empty.
    fn fill(&mut self, chunk: &mut Bytes, want: usize) {
        let take = want.saturating_sub(self.partial.len()).min(chunk.len());
        self.partial.extend_from_slice(&chunk.split_to(take));
    }
}

fn packet_len(data: &[u8]) -> usize {
    u16::from_be_bytes([data[0], data[1]]) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packets() -> Vec<Bytes> {
        vec![
            Bytes::from_static(b"hello"),
            Bytes::new(),
            Bytes::from(vec![7; 300]),
            Bytes::from_static(b"x"),
            Bytes::from(vec![9; MAX_PACKET]),
            Bytes::from_static(b"end"),
        ]
    }

    fn decode(chunks: impl IntoIterator<Item = Bytes>) -> Vec<Bytes> {
        let mut decoder = FrameDecoder::default();
        let mut out = Vec::new();
        for chunk in chunks {
            decoder.push(chunk, |packet| out.push(packet));
        }
        assert!(decoder.partial.is_empty(), "bytes left over");
        out
    }

    #[test]
    fn round_trips_in_one_chunk() {
        let encoded = encode(&packets());
        assert_eq!(encoded.packets, 6);
        assert_eq!(encoded.oversized, 0);
        assert_eq!(decode([encoded.data]), packets());
    }

    #[test]
    fn round_trips_split_at_every_point() {
        let data = encode(&packets()).data;
        for split in 0..=data.len() {
            let chunks = [data.slice(..split), data.slice(split..)];
            assert_eq!(decode(chunks), packets(), "split at {split}");
        }
    }

    #[test]
    fn round_trips_one_byte_at_a_time() {
        let data = encode(&packets()[..4]).data;
        let chunks = (0..data.len()).map(|i| data.slice(i..i + 1));
        assert_eq!(decode(chunks), packets()[..4]);
    }

    #[test]
    fn skips_oversized_packets() {
        let packets = [
            Bytes::from_static(b"a"),
            Bytes::from(vec![0; MAX_PACKET + 1]),
            Bytes::from_static(b"b"),
        ];
        let encoded = encode(&packets);
        assert_eq!(encoded.packets, 2);
        assert_eq!(encoded.payload, 2);
        assert_eq!(encoded.oversized, 1);
        assert_eq!(decode([encoded.data]), [&b"a"[..], &b"b"[..]]);
    }
}
