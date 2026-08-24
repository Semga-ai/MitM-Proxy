use std::{
    io::{Error, Read},
    num::NonZeroUsize,
};

use bytes::BytesMut;
use flate2::read::ZlibDecoder;
use tokio_util::codec::Decoder;


pub struct FragmentCodec {}

pub struct PacketFrame {
    pub bytes: BytesMut,
    pub offset: usize,
}

impl Decoder for FragmentCodec {
    type Item = PacketFrame;
    type Error = Error;

    #[inline(always)]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 2 {
            return Ok(None);
        }

        let mut position = 0;

        let mut full_lenght: u32 = 0;

        loop {
            if src.len() <= position {
                return Ok(None);
            }
            let current_byte = src[position];

            full_lenght |= ((current_byte & 0x7F) as u32) << (position * 7);

            position += 1;
            if (current_byte & 0x80) == 0 {
                break;
            }

            if position >= 5 {
                return Err(Error::new(
                    std::io::ErrorKind::FileTooLarge,
                    "LEB128 so big",
                ));
            }
        }

        let total_len: usize = position + full_lenght as usize;

        if src.len() < total_len {
            return Ok(None);
        }

        let frame = PacketFrame {
            bytes: src.split_to(total_len as usize),
            offset: position,
        };

        return Ok(Some(frame));
    }
}

#[inline(always)]
pub fn get_compressed_length(data: &mut PacketFrame, is_compressed: bool) -> Option<NonZeroUsize> {
    if is_compressed {
        let mut position = 0;
        let mut compressed_data: u32 = 0;
        let bytes: &[u8] = data.bytes[data.offset..].as_ref();

        loop {
            let current_byte = bytes.get(position)?;

            compressed_data |= ((current_byte & 0x7F) as u32) << (position * 7);

            position += 1;
            if (current_byte & 0x80) == 0 {
                break;
            }

            if position >= 5 {
                return None;
            }
        }
        data.offset += position;

        NonZeroUsize::new(compressed_data as usize)?;
    }

    None
}

#[inline(always)]
pub fn uncompress(frame: &mut PacketFrame, buffer: &mut [u8]) -> Result<(), ()> {
    ZlibDecoder::new(&frame.bytes[frame.offset..])
        .read_exact(buffer)
        .map_err(|_| ())?;
    Ok(())
}

#[inline(always)]
pub fn final_encode(
    frame: &mut PacketFrame,
    bytes: Option<&mut BytesMut>,
    length: usize,
) -> Result<FinalPacket, ()> {
    let cur_bytes;
    let is_compressed;
    match bytes {
        Some(v) => {
            cur_bytes = &v[frame.offset..length];
            is_compressed = true;
        }
        None => {
            cur_bytes = &frame.bytes[frame.offset..frame.bytes.len()];
            is_compressed = false;
        }
    }

    let mut position = 0;
    let mut id_data: u32 = 0;

    loop {
        let current_byte = cur_bytes.get(position).ok_or(())?;
        id_data |= ((current_byte & 0x7F) as u32) << (position * 7);

        position += 1;
        if (current_byte & 0x80) == 0 {
            break;
        }

        if position >= 5 {
            return Err(());
        }
    }

    frame.offset += position;

    return Ok(FinalPacket {
        id: id_data,
        length: length,
        is_compressed: is_compressed,
        is_need_send: true,
    });
}

pub struct FinalPacket {
    pub id: u32,
    pub length: usize,
    pub is_compressed: bool,
    pub is_need_send: bool,
}
