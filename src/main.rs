use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
mod codecs;

use bytes::BytesMut;

use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::FramedRead;

use crate::codecs::*;

static LISTENER_ADDRESS: &str = "127.0.0.1:25565";
static CONNECTION_ADDRESS: &str = "";

static GLOBAL_STATE: AtomicU8 = AtomicU8::new(2);
static IS_COMPRESSED: AtomicBool = AtomicBool::new(false);

static BUFFER_SIZE: usize = 8 * 1024 * 1024;

#[inline(always)]
fn final_encode(
    frame: &mut PacketFrame,
    bytes: Option<&mut BytesMut>,
    length: usize,
    is_compressed: bool,
) -> Result<FinalPacket, ()> {
    let cur_bytes;
    match bytes {
        Some(v) => {
            cur_bytes = &v[frame.offset..length];
        }
        None => {
            cur_bytes = &frame.bytes[frame.offset..frame.bytes.len()];
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

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind(LISTENER_ADDRESS).await.unwrap();

    loop {
        //
        //RESET STATES
        //

        GLOBAL_STATE.store(2, Ordering::Relaxed);
        IS_COMPRESSED.store(false, Ordering::Relaxed);

        //
        //WORK WITH SOCKETS
        //

        let (socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let (reader_socket, writer_socket) = socket.into_split();

        let stream = TcpStream::connect(CONNECTION_ADDRESS).await.unwrap();
        stream.set_nodelay(true).unwrap();

        let (reader_stream, writer_stream) = stream.into_split();

        //S2C
        tokio::spawn(async move {
            let mut frame = FramedRead::new(reader_stream, FragmentCodec {});
            let mut writer = writer_socket;
            let mut uncompressed_bytes_buffer = BytesMut::zeroed(BUFFER_SIZE);
            uncompressed_bytes_buffer.clear();

            while let Some(Some(mut packet_frame)) = frame.next().await.map(|r| r.ok()) {
                let compressed_length =
                    get_compressed_length(&mut packet_frame, IS_COMPRESSED.load(Ordering::Relaxed));
                let mut val: FinalPacket;

                if let Some(length) = compressed_length.map(|q| q.get()).filter(|&vl| {
                    let of = packet_frame.offset;
                    uncompress(&mut packet_frame, &mut uncompressed_bytes_buffer[of..vl]).is_ok()
                }) {
                    val = final_encode(
                        &mut packet_frame,
                        Some(&mut uncompressed_bytes_buffer),
                        length,
                        true,
                    )
                    .unwrap();
                } else {
                    let lng = packet_frame.bytes.len();
                    val = final_encode(&mut packet_frame, None, lng, false).unwrap();
                }

                let payload: &[u8] = match val.is_compressed {
                    false => &packet_frame.bytes[packet_frame.offset..val.length],
                    true => &uncompressed_bytes_buffer[packet_frame.offset..val.length],
                };

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
                        //121
                        let pattern = b"\xc2\xa7n\xc2\xa7o\xc2\xa7m";
                        if payload
                            .windows(pattern.len())
                            .any(|window| window == pattern)
                        {
                            val.is_need_send = false;
                            println!("{},{}", hex::encode_upper(payload), val.id);
                        };

                        let pattern_fair = b"\xc2\xa7f\xc2\xa7a\xc2\xa7i\xc2\xa7r";
                        if payload
                            .windows(pattern_fair.len())
                            .any(|window| window == pattern_fair)
                        {
                            val.is_need_send = false;
                            println!("{},{}", hex::encode_upper(payload), val.id);
                        }
                    }
                    _ => {}
                }

                if val.is_need_send {
                    if let Err(e) = writer.write_all_buf(&mut packet_frame.bytes).await {
                        println!("{}", e);
                        break;
                    }
                }
            }
        });

        //C2S
        tokio::spawn(async move {
            let mut frame = FramedRead::new(reader_socket, FragmentCodec {});
            let mut writer = writer_stream;
            let mut uncompressed_bytes_buffer = BytesMut::zeroed(BUFFER_SIZE);
            uncompressed_bytes_buffer.clear();

            while let Some(Some(mut packet_frame)) = frame.next().await.map(|r| r.ok()) {
                let compressed_length =
                    get_compressed_length(&mut packet_frame, IS_COMPRESSED.load(Ordering::Relaxed));
                let mut val: FinalPacket;

                if let Some(length) = compressed_length.map(|q| q.get()).filter(|&vl| {
                    let of = packet_frame.offset;
                    uncompress(&mut packet_frame, &mut uncompressed_bytes_buffer[of..vl]).is_ok()
                }) {
                    val = final_encode(
                        &mut packet_frame,
                        Some(&mut uncompressed_bytes_buffer),
                        length,
                        true,
                    )
                    .unwrap();
                } else {
                    let lng = packet_frame.bytes.len();
                    val = final_encode(&mut packet_frame, None, lng, false).unwrap();
                }

                let payload: &[u8] = match val.is_compressed {
                    false => &packet_frame.bytes[packet_frame.offset..val.length],
                    true => &uncompressed_bytes_buffer[packet_frame.offset..val.length],
                };

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
                        //121
                        let pattern = b"\xc2\xa7n\xc2\xa7o\xc2\xa7m";
                        if payload
                            .windows(pattern.len())
                            .any(|window| window == pattern)
                        {
                            val.is_need_send = false;
                            println!("{},{}", hex::encode_upper(payload), val.id);
                        };

                        let pattern_fair = b"\xc2\xa7f\xc2\xa7a\xc2\xa7i\xc2\xa7r";
                        if payload
                            .windows(pattern_fair.len())
                            .any(|window| window == pattern_fair)
                        {
                            val.is_need_send = false;
                            println!("{},{}", hex::encode_upper(payload), val.id);
                        }
                    }
                    _ => {}
                }

                if val.is_need_send {
                    if let Err(e) = writer.write_all_buf(&mut packet_frame.bytes).await {
                        println!("{}", e);
                        break;
                    }
                }
            }
        });
    }
}
