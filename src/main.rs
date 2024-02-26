use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_connection(stream);
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

// Note: `fn fn_name(var: mut Type)` is invalid. mut is used to denote mutability
// of variables and references, not types.
fn handle_connection(mut stream: TcpStream) {
    let mut read_buf = Vec::with_capacity(512);

    loop {
        // Rust differentiates between Vec<T> and &mut Vec<T>. Implicit coercion
        // from Vec<T> to &mut Vec<T> doesn't occur, but &mut Vec<T> can be coerced
        // to &mut U if Vec<T> implements DerefMut<Target=U>.
        match stream.read(&mut read_buf) {
            Ok(n) => {
                // Check if no bytes were read from the stream
                // if n == 0 {
                //     println!("Sleeping to read more data");
                //     thread::sleep(std::time::Duration::from_millis(500));
                //     continue;
                // }
                handle_data(&mut stream, &read_buf);
                // handle_data(&mut stream, &read_buf);
                break;
            }
            Err(_) => todo!(),
        }
    }
}

fn handle_data(stream: &mut TcpStream, read_buf: &[u8]) {
    let res = String::from_utf8(read_buf.to_owned()).unwrap();
    println!("{res:?}");

    let ping_response = "+PONG\r\n";
    let ping_response_size = ping_response.len();

    // if res.contains("PING") {
    match stream.write(ping_response.as_bytes()) {
        Ok(n) if n == ping_response_size => {
            println!("Successfully ponged!");
        }
        _ => {
            println!("Failed to pong :(")
        }
    }

    stream.flush().unwrap()
    // }
}
