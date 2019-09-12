use std::net::{TcpListener, TcpStream};
use std::io::Read;
use std::time::{Instant, Duration};
use std::sync::{Arc, RwLock};
use std::ops::Add;

// https://raft.github.io/raft.pdf
fn main() {
    println!("Hello, Raft!");
    let args: Vec<String> = std::env::args().collect();
    println!("Command line args: {:?}", args);
    let network = Network::new(&args);
    println!("Network: {:?}", network);

    // When servers start up, they begins as followers
    let server_state = Arc::new(RwLock::new(ServerState::new()));
    let _state = State::new();

    let heartbeat_received_at = Arc::new(RwLock::new(HeartbeatReceivedAt::new()));

    let leader_election = LeaderElection::new(
        server_state.clone(),
        heartbeat_received_at.clone()
    );
    let _leader_election_handle = std::thread::spawn(move || {
        leader_election.start();
    });

    let mut rpc_handler = RpcHandler { network: &network };
    rpc_handler.listen();
}

#[derive(Debug)]
struct Network {
    port: String,
    nodes: Box<[String]>,
}

impl Network {
    fn new(args: &Vec<String>) -> Self {
        Self {
            port: args[1].clone(),
            nodes: args[1..].to_vec().into_boxed_slice(),
        }
    }
}

struct ServerState {
    value: ServerStateValue,
}

impl ServerState {
    fn new() -> Self {
        Self { value: ServerStateValue::Follower }
    }

    fn to_candidate(&mut self) {
        assert!(self.value == ServerStateValue::Follower);

        println!("Server state has been changed from Follower to Candidate");
        self.value = ServerStateValue::Candidate;
    }
}

// see Figure 4: Server states
// - Followers only respond to requests from other servers.
//   - If a follower receives no communication, it becomes a candidate and initiates an election.
// - A candidate that receives votes from a majority of the full cluster becomes the new leader.
// - Leaders typically operate until they fail.
#[derive(PartialEq)]
enum ServerStateValue {
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

struct RpcHandler<'a> {
    network: &'a Network,
}

impl RpcHandler<'_> {
    fn listen(&mut self) {
        let address = format!("127.0.0.1:{}", self.network.port);
        println!("RpcHandler is listening on {}", address);
        let listener = TcpListener::bind(address).unwrap();

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

struct LeaderElection {
    server_state: Arc<RwLock<ServerState>>,
    election_timeout: Duration,
    heartbeat_received_at: Arc<RwLock<HeartbeatReceivedAt>>,
}

struct HeartbeatReceivedAt {
    value: Instant,
}

impl HeartbeatReceivedAt {
    fn new() -> Self {
        Self {
            value: Instant::now()
        }
    }

    fn add(&self, duration: Duration) -> Instant {
        self.value.add(duration)
    }

    fn reset(&mut self) {
        self.value = Instant::now();
    }
}

impl LeaderElection {
    fn new(
        server_state: Arc<RwLock<ServerState>>,
        heartbeat_received_at: Arc<RwLock<HeartbeatReceivedAt>>
    ) -> Self {
        Self {
            server_state,
            election_timeout: Duration::from_secs(3), // TODO: Randomize per node
            heartbeat_received_at,
        }
    }

    fn start(self) {
        loop {
            let timeout = self.heartbeat_received_at
                .read()
                .unwrap()
                .add(self.election_timeout);
            let now = Instant::now();

            if now > timeout {
                // TODO
                println!("Receives no communication over a period `election timeout`.");
                self.server_state.write().unwrap().to_candidate();

                println!("Reset the heartbeat_received_at");
                self.heartbeat_received_at.write().unwrap().reset();
            } else {
                std::thread::sleep(timeout - now);
            }
        }
    }
}