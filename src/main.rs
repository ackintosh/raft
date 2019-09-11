use std::net::{TcpListener, TcpStream};
use std::io::Read;

// https://raft.github.io/raft.pdf
fn main() {
    println!("Hello, Raft!");

    // When servers start up, they begins as followers
    let _server_state = ServerState::Follower;
    let _state = State::new();

    let mut rpc_handler = RpcHandler { port: "8080".to_owned() };
    rpc_handler.listen();
}

// see Figure 4: Server states
// - Followers only respond to requests from other servers.
//   - If a follower receives no communication, it becomes a candidate and initiates an election.
// - A candidate that receives votes from a majority of the full cluster becomes the new leader.
// - Leaders typically operate until they fail.
enum ServerState {
    Follower,
    Candidate,
    Leader,
}

// Persistent state on all servers
// (Updated on stable storage before responding to RPCs)
struct State {
    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    current_term: u64,
    // candidateId that received vote in current term (or null if none)
    voted_for: Option<String>,
    // log entries; each entry contains command for state machine,
    // and term when entry was received by leader (first index is 1)
    logs: Vec<String>,
}

impl State {
    fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            logs: vec![],
        }
    }
}

struct RpcHandler {
    port: String,
}

impl RpcHandler {
    fn listen(&mut self) {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port)).unwrap();

        for stream in listener.incoming() {
            self.handle(&stream.unwrap());
        }
    }

    fn handle(&mut self, mut stream: &TcpStream) {
        let mut buffer = [0u8; 512];
        let size = stream.read(&mut buffer).unwrap();
        let body = String::from_utf8_lossy(&buffer[..size]).to_string();

        println!("Rpc message body: {}", body);
    }
}