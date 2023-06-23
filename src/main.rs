use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, Duration, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;


struct Command {
    name: CommandName,
    operands: Vec<String>
}

struct ValueAndExpiration {
    value: String,
    expiration: Option<u128>
}

type KeyValueDict = Arc<Mutex<HashMap<String, ValueAndExpiration>>>;

fn dispatcher(command: &Command, redis_dict: &KeyValueDict) -> String {
    match command.name {
        CommandName::PING => {
            return String::from("+PONG\r\n");
        },
        CommandName::ECHO => {
            return format!("+{}\r\n", command.operands[0]);
        },
        CommandName::SET => {
            let value: ValueAndExpiration;
            if command.operands.len() == 2 {
                println!("Command just saves the key, no expiration time {}: {}", command.operands[0], command.operands[1]);
                value = ValueAndExpiration{
                    value: command.operands[1].to_string(),
                    expiration: None,
                };
            } else {
                println!("Command saves the key, with expiration time {}: {} ({})", command.operands[0], command.operands[1], command.operands[2]);
                value = ValueAndExpiration{
                    value: command.operands[1].to_string(),
                    expiration: Some(
                        SystemTime::now().checked_add(
                            Duration::from_millis(
                                command.operands[2].parse().unwrap())
                        ).unwrap()
                        .duration_since(UNIX_EPOCH).unwrap().as_millis()
                    ),
                };
            }
            {
                let mut dict = redis_dict.lock().unwrap();
                dict.insert(command.operands[0].to_string(), value);
            }
            return String::from("+OK\r\n");
        },
        CommandName::GET => {
            let key = &command.operands[0];
            let dict = redis_dict.lock().unwrap();
            match dict.get(key) {
                Some(value) => {
                    match value.expiration {
                        Some(exp) => {
                            let time_now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                            if time_now > exp {
                                println!("Value with key {:?} has expired. Value was {:?}. Expiration time was {:?} < time now {:?}", key, value.value, exp, time_now);
                                return String::from("$-1\r\n");
                            }
                            return construct_return_redis_string(&value.value);
                        },
                        None => {
                            return construct_return_redis_string(&value.value);
                        },
                    }
                    
                },
                None => {
                    return String::from("$-1\r\n");
                },
            }
        },
    } 
}

fn construct_return_redis_string(val: &String) -> String {
    println!("key to return {}", val);
    return format!("${}\r\n{}\r\n", val.len(), val);
}

#[tokio::main]
async fn main() {
    let listener: TcpListener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let redis_dict: KeyValueDict = Arc::new(Mutex::new(HashMap::new()));
    
    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let redis_dict_ref = Arc::clone(&redis_dict);
        tokio::spawn(async move {
            println!("accepted new connection");
            let mut command: Command;
            loop {
                let mut buf = [0; 512];
                let bytes_read = stream.read(&mut buf).await.unwrap();
                if bytes_read == 0 {
                    println!("client closed the connection");
                    break;
                } else {
                    command = parse_redis_command(&buf);
                }
                let response = dispatcher(&command, &redis_dict_ref);
                stream.write(response.as_bytes()).await.unwrap();
            }
        });
    }
}

fn parse_bulk_string(buf: &[u8; 512], ptr: &mut usize) -> String {
    assert!(buf[*ptr] == ('$' as u8), "expect bulk strings to start with $");
    *ptr += 1;
    let mut len: u8 = 0;
    let zero: u8 = '0' as u8;
    while buf[*ptr] != ('\r' as u8) {
        len = len * 10 + (buf[*ptr] - zero);
        *ptr += 1;
    }
    *ptr += 2;
    let mut result = String::new();
    for _ in 0..len {
        result.push(char::from(buf[*ptr]));
        *ptr += 1;
    }
    *ptr += 2;
    result
}
enum CommandName {
    PING,
    ECHO,
    SET,
    GET,
}


fn parse_array(buf: &[u8; 512], ptr: &mut usize) -> Vec<String> {
    *ptr += 1;

    let mut len: u8 = 0;
    let zero: u8 = '0' as u8;
    while buf[*ptr] != ('\r' as u8) {
        len = len * 10 + (buf[*ptr] - zero);
        *ptr += 1;
    }
    *ptr += 2;

    let mut result: Vec<String> = Vec::new();
    for _ in 0..len {
        result.push(parse_bulk_string(buf, ptr));
    }
    result
}


fn parse_redis_command(buf: &[u8; 512]) -> Command {
    let mut ptr: usize = 0;
    let elements = parse_array(buf, &mut ptr);

    // ping
    if elements[0].to_lowercase() == "ping" {
        return Command{
            name: CommandName::PING,
            operands: vec![],
        };
    } else if elements[0].to_lowercase() == "echo" {
        return Command{
            name: CommandName::ECHO,
            operands: vec![elements[1].to_string()],
        };
    } else if elements[0].to_lowercase() == "set" {
        let mut operands = vec![elements[1].to_string(), elements[2].to_string()];
        if elements.len() > 3 {
            operands.push(elements[4].to_string());
        }
        return Command{
            name: CommandName::SET,
            operands: operands,
        };
    } else if elements[0].to_lowercase() == "get" {
        return Command{
            name: CommandName::GET,
            operands: vec![elements[1].to_string()],
        };
    } else {
        panic!("Unknown command received")
    }
}

