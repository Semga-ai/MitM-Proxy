mod codecs;
mod network;

use crate::network::{reset_states, tcp_c2s, tcp_s2c};
use tokio::net::{TcpListener, TcpStream};

static LISTENER_ADDRESS: &str = "127.0.0.1:25565";

static CONNECTION_ADDRESS: &str = "";

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind(LISTENER_ADDRESS).await.unwrap();

    loop {
        //
        //RESET STATES
        //

        reset_states();

        //
        //WORK WITH TCP SOCKETS
        //

        let (socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let (reader_socket, writer_socket) = socket.into_split();

        let stream = TcpStream::connect(CONNECTION_ADDRESS).await.unwrap();
        stream.set_nodelay(true).unwrap();

        let (reader_stream, writer_stream) = stream.into_split();

        //
        //TCP Tunneling
        //

        tokio::spawn(async move {
            tcp_s2c(reader_stream, writer_socket).await;
        });

        tokio::spawn(async move {
            tcp_c2s(writer_stream, reader_socket).await;
        });
    }
}
