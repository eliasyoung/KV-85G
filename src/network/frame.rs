use std::io::{Read, Write};
use crate::{CommandRequest, CommandResponse, KvError};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use bytes::{Buf, BufMut, BytesMut};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

// length info part use 4 bytes
pub const LEN_LEN: usize = 4;

// MAX FRAME SIZE is 2G since length info is 31 bit
const MAX_FRAME: usize = 2 * 1024 * 1024 * 1024;

// if payload larger than 1436 bytes, compress it
const COMPRESSION_LIMIT: usize = 1436;

// compress flag bit shows whether it's compressed or not
const COMPRESSION_BIT: usize = 1 << 31;

pub trait FrameCoder
    where
        Self: Message + Sized + Default,
{
    // Encode a Message to a frame
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        let size = self.encoded_len();

        if size >= MAX_FRAME {
            return Err(KvError::FrameError);
        }

        // write length info first
        buf.put_u32(size as _);

        // if need to be compressed
        if size > COMPRESSION_LIMIT {
            let mut buf1 = Vec::with_capacity(size);
            self.encode(&mut buf1)?;

            // split the 4 bytes length info first and clear it.
            // payload is the buf at first.
            let payload = buf.split_off(LEN_LEN);
            buf.clear();

            // compress it with gzip.
            let mut encoder = GzEncoder::new(payload.writer(), Compression::default());
            encoder.write_all(&buf1[..])?;

            // get the compressed BytesMut back after compression from gzip encoder
            let payload = encoder.finish()?.into_inner();
            debug!("Encode a frame: size {}({})", size, payload.len());

            // write the length info after compression
            buf.put_u32((payload.len() | COMPRESSION_BIT) as _);

            // merge the compressed BufMut back
            buf.unsplit(payload);

            Ok(())
        } else {
            self.encode(buf)?;
            Ok(())
        }
    }

    // Decode a frame to a Message
    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        let header = buf.get_u32() as usize;
        let (len, compressed) = decode_header(header);
        debug!("Got a frame: msg len {}, compressed {}", len, compressed);

        if compressed {
            // extraction
            let mut decoder = GzDecoder::new(&buf[..len]);
            let mut buf1 = Vec::with_capacity(len * 2);
            decoder.read_to_end(&mut buf1)?;
            buf.advance(len);
            Ok(Self::decode(&buf1[..buf1.len()])?)

        } else {
            let msg = Self::decode(&buf[..len])?;
            buf.advance(len);
            Ok(msg)
        }
    }
}

impl FrameCoder for CommandRequest {}
impl FrameCoder for CommandResponse {}

fn decode_header(header: usize) -> (usize, bool) {
    let len = header & !COMPRESSION_BIT;
    let compressed = header & COMPRESSION_BIT == COMPRESSION_BIT;
    (len, compressed)
}