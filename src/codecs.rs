use std::io::Error;

use bytes::{BytesMut};
use tokio_util::codec::Decoder;


pub struct FragmentCodec{}

pub struct PacketFrame{
    pub bytes: BytesMut,
    pub offset: usize
}

impl Decoder for FragmentCodec {
    type Item = PacketFrame;
    type Error = Error;
    
    #[inline(always)]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 1 {
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
                return Err(Error::new(std::io::ErrorKind::FileTooLarge, "LEB128 so big"));
            }


        }

        let total_len: usize = position + full_lenght as usize;


        if src.len() < total_len {
            return Ok(None);
        }


        
        let frame = PacketFrame {bytes: src.split_to(total_len as usize), offset: position};

        return Ok(Some(frame));
    }
}







pub struct FinalPacket {
    pub id: u32,
    pub offset: usize,
    pub is_need_send: bool
}

