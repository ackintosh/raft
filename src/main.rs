use std::net::{TcpListener, TcpStream};
use std::io::Read;

// https://raft.github.io/raft.pdf
fn main() {
    println!("Hello, Raft!");

    // When servers start up, they begins as followers
    let _state = ServerState::Follower;

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