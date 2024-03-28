use std::{
    collections::HashMap,
    fmt::format,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use base64::prelude::*;
use clap::Parser;
use rand::{distributions::Alphanumeric, Rng};

type Store = Arc<Mutex<HashMap<String, (String, Option<Instant>)>>>;

const REPL_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
const RDB_64: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

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
        let listener = TcpListener::bind((config.host.to_owned(), config.port.to_owned())).unwrap();
        let role = match &config.replicaof {
            Some(replica) => {
                let host = replica.first().expect("shoud contain host");
                let port = replica.last().expect("should contain port");
                let port: u16 = port.parse().expect("port should be valid");
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

        Server::init_handshake(&config, &role);
        println!("handshake success");

        Server {
            role,
            listener,
            store,
        }
    }

    fn init_handshake(config: &Config, role: &Role) {
        match role {
            Role::Master {
                master_replid,
                master_repl_offset,
            } => {
                return;
            }
            Role::Slave((master_host, master_port)) => {
                let self_port = config.port;
                let mut read_buf = vec![0; 1024];
                let mut stream =
                    TcpStream::connect((master_host.to_owned(), master_port.to_owned()))
                        .expect("failed to connect to master");

                // Do PING
                println!("init ping");
                let op = format!("*1\r\n$4\r\nping\r\n");
                stream
                    .write_all(op.as_bytes())
                    .expect("should be able to write to master");
                let _ = stream.read(&mut read_buf).expect("should get some message");
                println!("read_buf: {read_buf:?}");
                let resp = String::from_utf8(read_buf.to_owned()).unwrap();
                println!("resp: {resp:?}");
                if !resp.to_lowercase().contains("pong") {
                    panic!("did not receive pong");
                }
                // read_buf.iter_mut().for_each(|x| *x = 0);

                // Do REPLCONF
                println!("init first replconf");
                let op = format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n",
                    self_port
                );
                stream
                    .write_all(op.as_bytes())
                    .expect("should be able to write to master");
                let _ = stream.read(&mut read_buf).expect("should get some message");
                println!("read_buf: {read_buf:?}");
                let resp = String::from_utf8(read_buf.to_owned()).unwrap();
                println!("resp: {resp:?}");
                if !resp.to_lowercase().contains("ok") {
                    panic!("did not receive ok");
                }
                // read_buf.iter_mut().for_each(|x| *x = 0);

                // Do REPLCONF
                println!("init second replconf");
                let op = format!("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
                stream
                    .write_all(op.as_bytes())
                    .expect("should be able to write to master");
                let _ = stream.read(&mut read_buf).expect("should get some message");
                println!("read_buf: {read_buf:?}");
                let resp = String::from_utf8(read_buf.to_owned()).unwrap();
                println!("resp: {resp:?}");
                if !resp.to_lowercase().contains("ok") {
                    panic!("did not receive ok");
                }
                // read_buf.iter_mut().for_each(|x| *x = 0);

                // Do PSYNC
                println!("init psync");
                let op = format!("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
                stream
                    .write_all(op.as_bytes())
                    .expect("should be able to write to master");
                let _ = stream.read(&mut read_buf).expect("should get some message");
                println!("read_buf: {read_buf:?}");
                // let resp = String::from_utf8(read_buf.to_owned()).unwrap();
                // println!("resp: {resp:?}");
                // if !resp.to_lowercase().contains("fullresync") {
                //     panic!("did not receive fullresync");
                // }
            }
        }
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
                } else if message.contains("replconf") {
                    // TODO: do this only for Master
                    handle_replconf(stream, arr_iter)
                } else if message.contains("psync") {
                    handle_psync(stream, arr_iter)
                }
            }
        }
    }

    fn handle_ping(stream: &mut TcpStream) {
        let _ = stream.write_all("+PONG\r\n".as_bytes());
    }

    fn handle_echo<'a, T>(stream: &mut TcpStream, mut it: T)
    where
        T: Iterator<Item = &'a Resp>,
    {
        let message = it.next().unwrap().get_string().unwrap();
        let len = message.len();
        let op = format!("${len}\r\n{message}\r\n");
        let _ = stream.write_all(op.as_bytes());
    }

    fn handle_info<'a, T>(stream: &mut TcpStream, mut it: T, role: &Role)
    where
        T: Iterator<Item = &'a Resp>,
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
        T: Iterator<Item = &'a Resp>,
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
        T: Iterator<Item = &'a Resp>,
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

    fn handle_replconf<'a, T>(stream: &mut TcpStream, mut it: T)
    where
        T: Iterator<Item = &'a Resp>,
    {
        let _ = stream.write_all("+OK\r\n".as_bytes());
    }

    fn handle_psync<'a, T>(stream: &mut TcpStream, mut it: T)
    where
        T: Iterator<Item = &'a Resp>,
    {
        let rdb = BASE64_STANDARD.decode(RDB_64).unwrap();
        // let rdb_str: String = rdb.iter().map(|n| format!("{n:08b}")).collect();
        let op = format!("+FULLRESYNC {REPL_ID} 0\r\n${}\r\n", rdb.len());
        let _ = stream.write(op.as_bytes());
        let _ = stream.write(&rdb);
        println!("writing: {op}");
        let _ = stream.write_all(op.as_bytes());
    }
}

/// Implementation of the REDIS protocol
#[derive(Debug, PartialEq, Eq)]
enum Resp {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Resp>),
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

    fn parse_array(input: &str) -> (Vec<Resp>, String) {
        let (data, residual) = input.split_once("\r\n").unwrap();
        let size: usize = data.parse().unwrap();

        let mut elements = Vec::new();
        let mut residual = residual.to_owned();

        for _ in 0..size {
            let (item, res) = Resp::new(&residual);
            elements.push(item);
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
            Resp::BulkString("hello".to_owned()),
            Resp::BulkString("world".to_owned()),
        ])
    );
}

#[test]
fn decode() {
    let buf = [
        43, 70, 85, 76, 76, 82, 69, 83, 89, 78, 67, 32, 120, 117, 116, 100, 100, 48, 106, 117, 48,
        52, 48, 102, 118, 99, 122, 115, 98, 108, 114, 98, 49, 106, 113, 106, 101, 54, 108, 98, 106,
        107, 115, 116, 55, 119, 120, 122, 99, 56, 113, 113, 32, 48, 13, 10, 36, 56, 56, 13, 10, 82,
        69, 68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114, 5, 55,
        46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64, 250, 5,
        99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117, 115, 101, 100, 45, 109, 101,
        109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101, 192, 0, 255, 240,
        110, 59, 254, 192, 255, 90, 162, 0,
    ];

    let string = String::from_utf16(&buf.to_vec()).unwrap();
    println!("{string:?}");
}
