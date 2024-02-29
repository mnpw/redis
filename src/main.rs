use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("New connection: {:?}", stream.peer_addr());
                handle_connection(stream);
            }
            Err(e) => {
                println!("error: {}", e);
                break;
            }
        }
    }
}

// Note: `fn fn_name(var: mut Type)` is invalid. mut is used to denote mutability
// of variables and references, not types.
fn handle_connection(mut stream: TcpStream) {
    let mut read_buf = Vec::with_capacity(512);
    // Sets TCP_NODELAY at kernel level. TCP_NODELAY basically disables Nagle's
    // algorithm.
    //
    // Nagle's algorithm combines several small outgoing messages and sends them
    // as a single packet to reduce the number of packets sent. This is
    // beneficial for reducing network congestion but can introduce latency.
    //
    // Disabling it with TCP_NODELAY is useful for applications that require
    // low latency and send small packets frequently.
    stream.set_nodelay(true).unwrap();

    loop {
        // Rust differentiates between Vec<T> and &mut Vec<T>. Implicit coercion
        // from Vec<T> to &mut Vec<T> doesn't occur, but &mut Vec<T> can be coerced
        // to &mut U if Vec<T> implements DerefMut<Target=U>.
        match stream.read(&mut read_buf) {
            Ok(_n) => {
                handle_data(&mut stream, &read_buf);
                break;
            }
            Err(_) => todo!(),
        }
    }
}

fn handle_data(stream: &mut TcpStream, read_buf: &[u8]) {
    let res = String::from_utf8(read_buf.to_owned()).unwrap();
    println!("Client says: {res:?}");

    let ping_response = "+PONG\r\n";

    match stream.write_all(ping_response.as_bytes()) {
        Ok(()) => {
            println!("Successfully ponged!");
        }
        _ => {
            println!("Failed to pong :(")
        }
    }

    stream.flush().unwrap();
}
