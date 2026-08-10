use std::io::{Error, Read};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
mod codecs;


use bytes::{ BytesMut};
use flate2::read::ZlibDecoder;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncWriteExt};
use tokio_util::codec::{ FramedRead};
use futures::StreamExt;

use crate::codecs::{FinalPacket, FragmentCodec, PacketFrame};

static LISTENER_ADDRESS: &str = "127.0.0.1:25565";
static CONNECTION_ADDRESS: &str = "";

static GLOBAL_STATE: AtomicU8 = AtomicU8::new(3);
static IS_COMPRESSED: AtomicBool = AtomicBool::new(false);

static BUFFER_SIZE: usize = 8 * 1024 * 1024;



#[inline(always)]
unsafe fn get_compressed_lenth(frame: &mut PacketFrame) -> Result<Option<u32>, Error> {
    if IS_COMPRESSED.load(Ordering::Relaxed) {
        let mut position = 0;
        let mut compressed_data: u32 = 0;

        unsafe {
            loop {
                
                let current_byte = frame.bytes.get_unchecked(position + frame.offset);

                compressed_data |= ((current_byte & 0x7F) as u32) << (position * 7);


                
                position += 1;
                if (current_byte & 0x80) == 0 {
                    break;
                }

                if position >= 5 {
                    return Err(Error::new(std::io::ErrorKind::FileTooLarge, "LEB128 so big"));
                }

            }
        }
        frame.offset += position;

        if compressed_data == 0 {
            return Ok(None);
        } else {
            return Ok(Some(compressed_data));
        }
    }
    return Ok(None);
}






#[inline(always)]
fn uncompress(frame: &mut BytesMut, buffer: &mut [u8], offset: usize) -> Result<(),()> {
    let mut zlib_decoder = ZlibDecoder::new(&frame[offset..]);

    if zlib_decoder.read_exact(buffer).is_ok() {
        return Ok(());
    } else {
        return Err(());
    }
}




#[inline(always)]
unsafe fn final_encode(offset: usize, bytes: &mut BytesMut, lenth: usize) -> Result<FinalPacket, Error> {
    let cur_bytes = &bytes[offset..lenth];

    let mut position = 0;
    let mut id_data: u32 = 0;

    unsafe {
        loop {
            let current_byte = cur_bytes.get_unchecked(position);
            id_data |= ((current_byte & 0x7F) as u32) << (position * 7);
            
            position += 1;
            if (current_byte & 0x80) == 0 {
                break;
            }

            if position >= 5 {
                return Err(Error::new(std::io::ErrorKind::FileTooLarge, "LEB128 so big"));
            }
        }
    }

    return Ok(FinalPacket {id: id_data, offset: position + offset, is_need_send: true});
}


#[tokio::main]
async fn main() {
    let listener = TcpListener::bind(LISTENER_ADDRESS).await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let (reader_socket,writer_socket) = socket.into_split();
    
        let stream = TcpStream::connect( CONNECTION_ADDRESS).await.unwrap();
        stream.set_nodelay(true).unwrap();
    
        let (reader_stream,writer_stream) = stream.into_split();
    
        //S2C
        tokio::spawn(async move {
            let mut frame= FramedRead::new(reader_stream, FragmentCodec {});
            let mut writer = writer_socket;
            let mut uncompressed_bytes_buffer = BytesMut::zeroed(BUFFER_SIZE);
            uncompressed_bytes_buffer.clear();
    
            while let Some(src) = frame.next().await {
                match src {
                    Ok(mut packet_frame) => {
                        let compressed_lenth = unsafe {
                            get_compressed_lenth(&mut packet_frame)
                        };

                        let mut data = &mut packet_frame.bytes;
                        let offset = packet_frame.offset;
                        let mut len = data.len();
                        if let Ok(Some(lenth)) = compressed_lenth {
                            if let Ok(_) = uncompress(data,&mut uncompressed_bytes_buffer[offset..lenth as usize], offset) {
                                data = &mut uncompressed_bytes_buffer;
                                len = offset + lenth as usize;
                            }
                        }

                        unsafe {
                            let mut val = final_encode(offset, data, len).unwrap();

                            //offset = val.offset;
                            let payload: &mut BytesMut = data;

                            match GLOBAL_STATE.load(Ordering::Relaxed) {
                                //LOGIN
                                2 => {
                                    if val.id == 3 {
                                        IS_COMPRESSED.store(true, Ordering::Relaxed);
                                    } else if val.id == 2 {
                                        GLOBAL_STATE.store(3, Ordering::Relaxed);
                                    }
                                }

                                //CONFIGURATION
                                3 => {
                                    if val.id == 3 {
                                        GLOBAL_STATE.store(4, Ordering::Relaxed);
                                    }
                                }

                                //PLAY
                                4 => {
                                    let pattern = b"\xc2\xa7n\xc2\xa7o\xc2\xa7m";
                                    if payload.windows(pattern.len()).any(|window| window == pattern) {
                                        val.is_need_send = false;
                                    };


                                    let pattern_fair = b"\xc2\xa7f\xc2\xa7a\xc2\xa7i\xc2\xa7r";
                                    if payload.windows(pattern_fair.len()).any(|window| window == pattern_fair) {
                                        val.is_need_send = false;
                                    }
                                }
                                _ => {

                                }
                            }

                            if val.is_need_send {
                                match writer.write_all_buf(&mut packet_frame.bytes).await {
                                    Err(e) => {
                                        println!("{e}");
                                        break;
                                    }
                                    Ok(_) => {}
                                }
                            }
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });
        

        //C2S
        tokio::spawn(async move {
            let mut frame= FramedRead::new(reader_socket, FragmentCodec {});
            let mut writer = writer_stream;
            let mut uncompressed_bytes_buffer = BytesMut::zeroed(BUFFER_SIZE);
            uncompressed_bytes_buffer.clear();
    
            while let Some(src) = frame.next().await {
                match src {
                    Ok(mut packet_frame) => {
                        let compressed_lenth = unsafe {
                            get_compressed_lenth(&mut packet_frame)
                        };

                        let mut data = &mut packet_frame.bytes;
                        let offset = packet_frame.offset;
                        let mut len = data.len();
                        if let Ok(Some(lenth)) = compressed_lenth {
                            if let Ok(_) = uncompress(data,&mut uncompressed_bytes_buffer[offset..lenth as usize],offset) {
                                data = &mut uncompressed_bytes_buffer;
                                len = offset + lenth as usize;
                            }
                        }

                        
                        unsafe {
                            let mut val = final_encode(offset, data, len).unwrap();

                            //offset = val.offset;
                            let payload: &mut BytesMut = data;

                            match GLOBAL_STATE.load(Ordering::Relaxed) {
                                //LOGIN
                                2 => {
                                    if val.id == 3 {
                                        IS_COMPRESSED.store(true, Ordering::Relaxed);
                                    } else if val.id == 2 {
                                        GLOBAL_STATE.store(3, Ordering::Relaxed);
                                    }
                                }

                                //CONFIGURATION
                                3 => {
                                    if val.id == 3 {
                                        GLOBAL_STATE.store(4, Ordering::Relaxed);
                                    }
                                }

                                //PLAY
                                4 => {
                                    let pattern = b"\xc2\xa7n\xc2\xa7o\xc2\xa7m";
                                    if payload.windows(pattern.len()).any(|window| window == pattern) {
                                        val.is_need_send = false;
                                    };


                                    let pattern_fair = b"\xc2\xa7f\xc2\xa7a\xc2\xa7i\xc2\xa7r";
                                    if payload.windows(pattern_fair.len()).any(|window| window == pattern_fair) {
                                        val.is_need_send = false;
                                    }
                                }
                                _ => {

                                }
                            }

                            if val.is_need_send {
                                match writer.write_all_buf(&mut packet_frame.bytes).await {
                                    Err(e) => {
                                        println!("{e}");
                                        break;
                                    }
                                    Ok(_) => {}
                                }
                            }
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });
    }
}