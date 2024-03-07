use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use rand::{distributions::Alphanumeric, Rng};

type Store = Arc<Mutex<HashMap<String, (String, Option<Instant>)>>>;

fn init_store() -> Store {
    Arc::new(Mutex::new(HashMap::new()))
}

#[derive(Parser, Debug)]
struct Config {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 6379)]
    port: u16,
    #[arg(long, num_args = 2, value_name = "REPLICA_HOST REPLICA_PORT")]
    replicaof: Option<Vec<String>>,
}

fn main() {
    let config = dbg!(Config::parse());
    let store = init_store();
    let server = Server::init(config, store);

    server.start();
}

#[derive(Clone)]
enum Role {
    Master {
        master_replid: String,
        master_repl_offset: usize,
    },
    Slave((String, u16)),
}

struct Server {
    role: Role,
    listener: TcpListener,
    store: Store,
}

impl Server {
    fn init(config: Config, store: Store) -> Self {
        let listener = TcpListener::bind((config.host, config.port)).unwrap();
        let role = match config.replicaof {
            Some(replica) => {
                let host = replica.first().expect("shoud contain host");
                let port = replica.last().expect("should contain port");
                let port: u16 = port.parse().expect("port should be valid");
                Server::init_handshake(host, port);
                Role::Slave((host.to_owned(), port))
            }
            None => {
                let random_string: String = rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(40)
                    .map(char::from)
                    .collect();

                Role::Master {
                    master_replid: random_string,
                    master_repl_offset: 0,
                }
            }
        };

        Server {
            role,
            listener,
            store,
        }
    }

    fn init_handshake(host: &String, port: u16) {
        let mut stream =
            TcpStream::connect((host.to_owned(), port)).expect("failed to connect to master");
        let op = format!("*1\r\n$4\r\nping\r\n");
        stream
            .write_all(op.as_bytes())
            .expect("should be able to write to master");
    }

    fn start(self) {
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("new connection: {:?}", stream.peer_addr());
                    let store = self.store.clone();
                    let role = self.role.clone();
                    thread::spawn(move || {
                        handle_connection(stream, store, role);
                    });
                }
                Err(e) => {
                    println!("error: {}", e);
                    break;
                }
            }
        }
    }
}

// Note: `fn fn_name(var: mut Type)` is invalid. mut is used to denote mutability
// of variables and references, not types.
fn handle_connection(mut stream: TcpStream, store: Store, role: Role) {
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
                handle_data(&mut stream, &buf, &store, &role);
                buf.iter_mut().for_each(|x| *x = 0);
            }
            Err(_) => todo!(),
        }
    }

    // TODO:
    // 1. handle all unwarps
    // 2. support creating of `Resp` message from &str
    // 3. clean up get and set operations
    fn handle_data(stream: &mut TcpStream, buf: &[u8], store: &Store, role: &Role) {
        let incoming_message =
            String::from_utf8(buf.to_owned()).expect("Failed to construct message");
        let incoming_message = incoming_message.trim_end().trim_end_matches('\0');
        println!("incoming message: {incoming_message:?}");

        let (resp, _residual) = Resp::new(incoming_message);
        match resp {
            Resp::SimpleString(s) => {
                if s.to_lowercase().contains("ping") {
                    handle_ping(stream)
                }
            }
            Resp::BulkString(s) => {
                if s.to_lowercase().contains("ping") {
                    handle_ping(stream)
                }
            }
            Resp::Array(arr) => {
                let mut arr_iter = arr.iter();

                let message = arr_iter
                    .next()
                    .unwrap()
                    .get_string()
                    .unwrap()
                    .to_lowercase();

                if message.contains("ping") {
                    handle_ping(stream);
                } else if message.contains("echo") {
                    handle_echo(stream, arr_iter);
                } else if message.contains("info") {
                    handle_info(stream, arr_iter, role)
                } else if message.contains("set") {
                    handle_set(stream, arr_iter, store)
                } else if message.contains("get") {
                    handle_get(stream, arr_iter, store)
                }
            }
        }
    }

    fn handle_ping(stream: &mut TcpStream) {
        let _ = stream.write_all("+PONG\r\n".as_bytes());
    }

    fn handle_echo<'a, T>(stream: &mut TcpStream, mut it: T)
    where
        T: Iterator<Item = &'a Box<Resp>>,
    {
        let message = it.next().unwrap().get_string().unwrap();
        let len = message.len();
        let op = format!("${len}\r\n{message}\r\n");
        let _ = stream.write_all(op.as_bytes());
    }

    fn handle_info<'a, T>(stream: &mut TcpStream, mut it: T, role: &Role)
    where
        T: Iterator<Item = &'a Box<Resp>>,
    {
        let info_type = it.next().unwrap().get_string().unwrap();
        if info_type == "replication" {
            let op = match role {
                Role::Master {
                    master_replid,
                    master_repl_offset,
                } => {
                    let count = 11 + 1 + 54 + 1 + 20;
                    format!("${count}\r\nrole:master\nmaster_replid:{master_replid}\nmaster_repl_offset:{master_repl_offset}\r\n")
                }
                Role::Slave(_) => format!("$10\r\nrole:slave\r\n"),
            };

            let _ = stream.write_all(op.as_bytes());
        }
    }

    fn handle_set<'a, T>(stream: &mut TcpStream, mut it: T, store: &Store)
    where
        T: Iterator<Item = &'a Box<Resp>>,
    {
        let key = it.next().unwrap().get_string().unwrap();
        let val = it.next().unwrap().get_string().unwrap();
        let mut s = store.lock().expect("Store is poisoned!");

        let expiry: Option<usize> = match it.next() {
            Some(exp) => {
                if exp.get_string().unwrap().to_lowercase().contains("px") {
                    it.next().unwrap().get_string().unwrap().parse().ok()
                } else {
                    None
                }
            }
            None => None,
        };

        let expiry_time = expiry.map(|delta| Instant::now() + Duration::from_millis(delta as u64));

        s.insert(key, (val, expiry_time));
        let _ = stream.write_all("+OK\r\n".as_bytes());
    }

    fn handle_get<'a, T>(stream: &mut TcpStream, mut it: T, store: &Store)
    where
        T: Iterator<Item = &'a Box<Resp>>,
    {
        let key = it.next().unwrap().get_string().unwrap();
        let s = store.lock().expect("Store is poisoned!");
        if let Some((val, expiry)) = s.get(&key) {
            if expiry.is_some() && Instant::now() > expiry.unwrap() {
                let _ = stream.write_all("$-1\r\n".as_bytes());
            } else {
                let len = val.len();
                let op = format!("${len}\r\n{val}\r\n");
                let _ = stream.write_all(op.as_bytes());
            }
        } else {
            let _ = stream.write_all("$-1\r\n".as_bytes());
        }
    }
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

    fn get_string(&self) -> Option<String> {
        match self {
            Resp::SimpleString(s) => Some(s.to_owned()),
            Resp::BulkString(s) => Some(s.to_owned()),
            _ => None,
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
