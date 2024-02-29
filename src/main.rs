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
    let incoming_message = incoming_message.trim_end().trim_end_matches('\0');
    println!("incoming message: {incoming_message:?}");

    let (resp, _residual) = Resp::new(incoming_message);
    match resp {
        Resp::SimpleString(s) => {
            if s.to_lowercase().contains("ping") {
                let _ = stream.write_all("+PONG\r\n".as_bytes());
            }
        }
        Resp::BulkString(s) => {
            if s.to_lowercase().contains("ping") {
                let _ = stream.write_all("+PONG\r\n".as_bytes());
            }
        }
        Resp::Array(arr) => {
            let mut arr_iter = arr.iter();
            // check if first message is echo
            if let Some(tp) = arr_iter.next() {
                let message = match tp.as_ref() {
                    Resp::SimpleString(s) => s,
                    Resp::BulkString(s) => s,
                    _ => unreachable!(),
                };

                if message.to_lowercase().contains("echo") {
                    // if there is a second message then take it and write that message back
                    if let Some(mes) = arr_iter.next() {
                        let message = match mes.as_ref() {
                            Resp::SimpleString(s) => s,
                            Resp::BulkString(s) => s,
                            _ => unreachable!(),
                        };
                        let size = message.len();
                        let op = format!("${size}\r\n{message}\r\n");
                        let _ = stream.write_all(op.as_bytes());
                    }
                }

                if message.contains("ping") {
                    let _ = stream.write_all("+PONG\r\n".as_bytes());
                }
            }
        }
    };
}

/// Implementation of the REDIS protocol
#[derive(Debug, PartialEq, Eq)]
enum Resp {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Box<Resp>>),
}

impl Resp {
    // Returns data type and residual data if any
    pub fn new(input: &str) -> (Self, String) {
        let (message_type, data) = input.split_at(1);

        match message_type {
            "+" => {
                let (d, res) = Self::parse_simple_string(data);
                (Resp::SimpleString(d), res)
            }
            "$" => {
                let (d, res) = Self::parse_bulk_string(data);
                (Resp::BulkString(d), res)
            }
            "*" => {
                let (d, res) = Self::parse_array(data);
                (Resp::Array(d), res)
            }
            _ => todo!(),
        }
    }

    fn parse_simple_string(input: &str) -> (String, String) {
        let (data, residual) = input.split_once("\r\n").unwrap();
        (data.to_owned(), residual.to_owned())
    }

    fn parse_bulk_string(input: &str) -> (String, String) {
        let (data, residual) = input.split_once("\r\n").unwrap();
        let size: usize = data.parse().unwrap();

        let (data, residual) = residual.split_once("\r\n").unwrap();
        assert_eq!(size, data.len());

        (data.to_owned(), residual.to_owned())
    }

    fn parse_array(input: &str) -> (Vec<Box<Resp>>, String) {
        let (data, residual) = input.split_once("\r\n").unwrap();
        let size: usize = data.parse().unwrap();

        let mut elements = Vec::new();
        let mut residual = residual.to_owned();

        for _ in 0..size {
            let (item, res) = Resp::new(&residual);
            elements.push(Box::new(item));
            residual = res;
        }

        (elements, residual.to_owned())
    }
}

#[test]
fn resp_test() {
    let simple = "+PONG\r\n";
    let (result, _) = Resp::new(simple);
    assert_eq!(result, Resp::SimpleString("PONG".to_owned()));

    let bulk = "$5\r\nhello\r\n";
    let (result, _) = Resp::new(bulk);
    assert_eq!(result, Resp::BulkString("hello".to_owned()));

    let array = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
    let (result, _) = Resp::new(array);
    assert_eq!(
        result,
        Resp::Array(vec![
            Box::new(Resp::BulkString("hello".to_owned())),
            Box::new(Resp::BulkString("world".to_owned())),
        ])
    );
}
