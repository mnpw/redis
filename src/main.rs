use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("new connection: {:?}", stream.peer_addr());
                thread::spawn(move || {
                    handle_connection(stream);
                });
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
    // Note: The buffer onto which TcpStream::read function is called must have
    // non-zero len! Initializing buffer like `let mut buffer =
    // Vec::with_capacity(1024)` will not work since the lenght of buffer will
    // still be zero.
    let mut buf = vec![0; 1024];
    // // Sets TCP_NODELAY at kernel level. TCP_NODELAY basically disables Nagle's
    // // algorithm.
    // //
    // // Nagle's algorithm combines several small outgoing messages and sends them
    // // as a single packet to reduce the number of packets sent. This is
    // // beneficial for reducing network congestion but can introduce latency.
    // //
    // // Disabling it with TCP_NODELAY is useful for applications that require
    // // low latency and send small packets frequently.
    stream.set_nodelay(true).unwrap();

    loop {
        // Rust differentiates between Vec<T> and &mut Vec<T>. Implicit coercion
        // from Vec<T> to &mut Vec<T> doesn't occur, but &mut Vec<T> can be coerced
        // to &mut U if Vec<T> implements DerefMut<Target=U>.
        match stream.read(&mut buf) {
            Ok(n) => {
                if n == 0 {
                    println!("waiting for more data...");
                    thread::sleep(Duration::from_secs_f64(0.5));
                    continue;
                }
                println!("sending pong...");
                handle_data(&mut stream, &buf);
            }
            Err(_) => todo!(),
        }
    }
}

fn handle_data(stream: &mut TcpStream, buf: &[u8]) {
    let incoming_message = String::from_utf8(buf.to_owned()).expect("Failed to construct message");
    println!("incoming message: {incoming_message:?}");
    let ping_response = "+PONG\r\n";

    if incoming_message.contains("ping") {
        match stream.write_all(ping_response.as_bytes()) {
            Ok(()) => {
                println!("Successfully ponged!");
            }
            _ => {
                println!("Failed to pong :(")
            }
        }
    }
}

#[test]
fn testing() {
    let data = [42, 49, 13, 10, 36, 52, 13, 10, 112, 105, 110, 103, 13, 10];
    let s = String::from_utf8(data.to_vec()).unwrap();

    println!("{s:?} {}", s.contains("ping"));
}
