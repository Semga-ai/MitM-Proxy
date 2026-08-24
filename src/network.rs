use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use crate::codecs::*;

use bytes::BytesMut;

use futures::StreamExt;
use tokio::{
    io::AsyncWriteExt,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};
use tokio_util::codec::FramedRead;

//
//SESSION STATUS
//

pub static GLOBAL_STATE: AtomicU8 = AtomicU8::new(2);
pub static IS_COMPRESSED: AtomicBool = AtomicBool::new(false);

pub static BUFFER_SIZE: usize = 8 * 1024 * 1024;

pub fn reset_states() {
    GLOBAL_STATE.store(2, Ordering::Relaxed);
    IS_COMPRESSED.store(false, Ordering::Relaxed);
}

//
//TCP Tunneling
//

pub async fn tcp_s2c(reader_stream: OwnedReadHalf, writer_socket: OwnedWriteHalf) {
    let mut frame = FramedRead::new(reader_stream, FragmentCodec {});
    let mut writer = writer_socket;
    let mut uncompressed_bytes_buffer = BytesMut::zeroed(BUFFER_SIZE);
    uncompressed_bytes_buffer.clear();

    //
    //ENCODING
    //

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

        //
        //Payload
        //

        let payload: &[u8] = match val.is_compressed {
            false => &packet_frame.bytes[packet_frame.offset..val.length],
            true => &uncompressed_bytes_buffer[packet_frame.offset..val.length],
        };

        //
        //Main logic
        //

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
}

pub async fn tcp_c2s(writer_stream: OwnedWriteHalf, reader_socket: OwnedReadHalf) {
    let mut frame = FramedRead::new(reader_socket, FragmentCodec {});
    let mut writer = writer_stream;
    let mut uncompressed_bytes_buffer = BytesMut::zeroed(BUFFER_SIZE);
    uncompressed_bytes_buffer.clear();

    //
    //ENCODING
    //

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

        //
        //Payload
        //

        let payload: &[u8] = match val.is_compressed {
            false => &packet_frame.bytes[packet_frame.offset..val.length],
            true => &uncompressed_bytes_buffer[packet_frame.offset..val.length],
        };

        //
        //Main logic
        //

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
                //121 id
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
}
