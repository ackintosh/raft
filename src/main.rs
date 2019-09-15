use std::net::{TcpListener, TcpStream, SocketAddr};
use std::io::{Read, Write};
use std::time::{Instant, Duration};
use std::sync::{Arc, RwLock, RwLockWriteGuard, RwLockReadGuard};
use std::ops::Add;

#[macro_use]
extern crate serde_derive;

// https://raft.github.io/raft.pdf
fn main() {
    println!("Hello, Raft!");
    let args: Vec<String> = std::env::args().collect();
    println!("Command line args: {:?}", args);
    let network = Arc::new(Network::new(&args));
    println!("Network: {:?}", network);
    let node_id = Arc::new(format!("node_{}_{}", "127.0.0.1", network.port));
    println!("NodeId: {:?}", node_id);

    // When servers start up, they begins as followers
    let server_state = Arc::new(RwLock::new(ServerState::new()));
    let state = Arc::new(RwLock::new(State::new()));

    let heartbeat_received_at = Arc::new(RwLock::new(HeartbeatReceivedAt::new()));

    let leader_election = LeaderElection::new(
        node_id.clone(),
        network.clone(),
        state.clone(),
        server_state.clone(),
        heartbeat_received_at.clone()
    );
    let _leader_election_handle = std::thread::spawn(move || {
        leader_election.start();
    });

    let heartbeat = Heartbeat {
        node_id: node_id.clone(),
        network: network.clone(),
        state: state.clone(),
        server_state: server_state.clone(),
    };
    let _heartbeat_handle = std::thread::spawn(move || {
        heartbeat.start();
    });

    let mut rpc_handler = RpcHandler {
        state: state.clone(),
        server_state: server_state.clone(),
        network: network.clone(),
        heartbeat_received_at: heartbeat_received_at.clone(),
    };
    rpc_handler.listen();
}

fn node_id(socket_addr: &SocketAddr) -> String {
    format!("node_{}_{}", socket_addr.ip().to_string(), socket_addr.port())
}

#[derive(Debug)]
struct Network {
    port: String,
    nodes: Box<[String]>,
    majority: i32,
}

impl Network {
    fn new(args: &Vec<String>) -> Self {
        let nodes = args[2..].to_vec().into_boxed_slice();
        let majority = math::round::ceil((&nodes.len() + 1) as f64 / 2f64, 0) as i32;

        Self {
            port: args[1].clone(),
            nodes,
            majority,
        }
    }

    fn is_majority(&self, i: i32) -> bool {
        i >= self.majority
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

    fn to_leader(&mut self) {
        assert!(self.value == ServerStateValue::Candidate);

        println!("Server state has been changed from Candidate to Leader");
        self.value = ServerStateValue::Leader;
    }

    fn to_follower(&mut self) {
        println!("Server state has been changed from {:?} to Follower", self.value);
        self.value = ServerStateValue::Follower;
    }
}

// see Figure 4: Server states
// - Followers only respond to requests from other servers.
//   - If a follower receives no communication, it becomes a candidate and initiates an election.
// - A candidate that receives votes from a majority of the full cluster becomes the new leader.
// - Leaders typically operate until they fail.
#[derive(Debug, PartialEq)]
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

    fn increment_term(&mut self) {
        self.current_term += 1;
    }

    fn voted_for(&mut self, node_id: &String) {
        self.voted_for = Some(node_id.to_string())
    }

    fn log_index(&self) -> u64 {
        // first index is 1
        (self.logs.len() + 1) as u64
    }
}

struct RpcHandler {
    state: Arc<RwLock<State>>,
    server_state: Arc<RwLock<ServerState>>,
    network: Arc<Network>,
    heartbeat_received_at: Arc<RwLock<HeartbeatReceivedAt>>
}

impl RpcHandler {
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

        let message = RpcMessage::from(&body);
        match message.r#type {
            RpcMessageType::RequestVote => self.handle_request_vote(stream, &message),
            RpcMessageType::AppendEntries => self.handle_append_entries(stream, &message),
        }
    }

    fn handle_request_vote(&self, mut stream: &TcpStream,  message: &RpcMessage) {
        let request_vote = RequestVote::from(&message.payload);

        let mut state = self.state
            .write().unwrap();

        let result = if self.verify_request_vote(
            &state,
            &request_vote
        ) {
            state.voted_for(&node_id(&stream.peer_addr().unwrap()));

            serde_json::to_string(&RequestVoteResult {
                term: state.current_term,
                vote_granted: true, // TODO
            }).unwrap()
        } else {
            serde_json::to_string(&RequestVoteResult {
                term: state.current_term,
                vote_granted: false,
            }).unwrap()
        };

        println!("request vote result: {:?}", result);
        stream.write(result.as_bytes()).unwrap();
    }

    fn verify_request_vote(
        &self,
        state: &RwLockWriteGuard<State>,
        request_vote: &RequestVote
    ) -> bool {
        // Reply false if term < currentTerm (§5.1)
        if request_vote.term < state.current_term {
            return false;
        }

        // If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log,
        // grant vote (§5.2, §5.4)
        match &state.voted_for {
            Some(candidate_id) => {
                if candidate_id.to_string() != request_vote.candidate_id {
                    println!("Rejected a request_vote as already voted to other candidate.");
                    false
                } else {
                    request_vote.last_log_index >= state.log_index()
                }
            }
            None => {
                request_vote.last_log_index >= state.log_index()
            }
        }
    }

    fn handle_append_entries(&self, mut strem: &TcpStream, message: &RpcMessage) {
        let append_entries = AppendEntries::from(&message.payload);

        let result = serde_json::to_string(&AppendEntriesResult {
            term: 1, // TODO
            success: self.verify_append_entries(&append_entries),
        }).unwrap();

        println!("AppendEntriesResult: {:?}", result);
        strem.write(result.as_bytes()).unwrap();

        self.server_state.write().unwrap().to_follower();
        self.heartbeat_received_at.write().unwrap().reset();
    }

    fn verify_append_entries(&self, _append_entries: &AppendEntries) -> bool {
        // TODO
        true
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcMessage {
    r#type: RpcMessageType,
    payload: String,
}

impl RpcMessage {
    fn create_request_vote(node_id: String, state: &RwLockWriteGuard<State>) -> Self {
        let payload = serde_json::to_string(&RequestVote::new(node_id, state)).unwrap();

        Self {
            r#type: RpcMessageType::RequestVote,
            payload,
        }
    }

    fn create_heartbeat(node_id: String, state: &RwLockReadGuard<State>) -> Self {
        let payload = serde_json::to_string(&AppendEntries::new(node_id, state)).unwrap();

        Self {
            r#type: RpcMessageType::AppendEntries,
            payload,
        }
    }

    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    fn from(str: &String) -> Self {
        serde_json::from_str(str).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum RpcMessageType {
    // Invoked by candidates to gather votes.
    RequestVote,
    // Invoked by leader to replicate log entries; also used as heartbeat.
    AppendEntries,
}

#[derive(Debug, Serialize, Deserialize)]
struct RequestVote {
    // candidate's term
    term: u64,
    // candidate requesting vote
    candidate_id: String,
    // index of candidate's last log entry
    last_log_index: u64,
    // term of candidate's log entry
    last_log_term: u64,
}

impl RequestVote {
    fn new(node_id: String, state: &RwLockWriteGuard<State>) -> Self {
        Self {
            term: state.current_term,
            candidate_id: node_id.to_string(),
            last_log_index: state.log_index(),
            last_log_term: 1, // TODO
        }
    }

    fn from(str: &String) -> Self {
        serde_json::from_str(str).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RequestVoteResult {
    // currentTerm, for candidate to update itself
    term: u64,
    // true means candidate received vote
    vote_granted: bool,
}

impl RequestVoteResult {
    fn from(bytes: &[u8]) -> Self {
        let str = String::from_utf8_lossy(bytes).to_string();
        serde_json::from_str(&str).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct AppendEntries {
    // leader's term
    term: u64,
    // so follower can redirect clients
    leader_id: String,
    // index of log entry immediately proceeding new ones
    prev_log_index: u64,
    // term of prevLogIndex entry
    prev_log_term: u64,
    // log entries to store (empty for heartbeat; may send more than one for efficiency)
    entries: Vec<String>,
    // leader's commitIndex
    leader_commit: u64,
}

impl AppendEntries {
    fn new(node_id: String, state: &RwLockReadGuard<State>) -> Self {
        Self {
            term: state.current_term,
            leader_id: node_id.to_string(),
            prev_log_index: 0, // TODO
            prev_log_term: 0, // TODO
            entries: vec![],
            leader_commit: 0, // TODO
        }
    }

    fn from(str: &String) -> Self {
        serde_json::from_str(str).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct AppendEntriesResult {
    // currentTerm, for leader to update itself
    term: u64,
    // true if follower contained entry matching prevLogIndex and prevLogTerm
    success: bool,
}

impl AppendEntriesResult {
    fn from(bytes: &[u8]) -> Self {
        let str = String::from_utf8_lossy(bytes).to_string();
        serde_json::from_str(&str).unwrap()
    }
}

struct LeaderElection {
    node_id: Arc<String>,
    network: Arc<Network>,
    state: Arc<RwLock<State>>,
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
        println!("HeartbeatReceivedAt has been updated");
        self.value = Instant::now();
    }
}

impl LeaderElection {
    fn new(
        node_id: Arc<String>,
        network: Arc<Network>,
        state: Arc<RwLock<State>>,
        server_state: Arc<RwLock<ServerState>>,
        heartbeat_received_at: Arc<RwLock<HeartbeatReceivedAt>>
    ) -> Self {
        Self {
            node_id,
            network,
            state,
            server_state,
            election_timeout: Duration::from_secs(3), // TODO: Randomize per node
            heartbeat_received_at,
        }
    }

    fn start(&self) {
        loop {
            if self.server_state.read().unwrap().value != ServerStateValue::Follower {
                std::thread::sleep(self.election_timeout);
                continue;
            }

            let timeout = self.heartbeat_received_at
                .read()
                .unwrap()
                .add(self.election_timeout);
            let now = Instant::now();

            if now > timeout {
                println!("Receives no communication over a period `election timeout`.");

                if self.start_election() {
                    println!("Received votes from a majority of the servers in the full cluster for the same term.");
                    self.server_state.write().unwrap().to_leader();
                }

                println!("Reset the heartbeat_received_at");
                self.heartbeat_received_at.write().unwrap().reset();
            } else {
                std::thread::sleep(timeout - now);
            }
        }
    }

    fn start_election(&self) -> bool {
        println!("The election has been started");
        // To begin an election, a follower increments its current term and transitions to candidate state.
        let mut state = self.state.write().unwrap();
        state.increment_term();
        state.voted_for(&self.node_id);
        self.server_state.write().unwrap().to_candidate();

        let message = RpcMessage::create_request_vote(
            self.node_id.to_string(),
            &state
        ).to_string();

        let mut granted_count = 0;
        for node in self.network.nodes.iter() {
            match self.send_request_vote(node, message.as_bytes()) {
                Ok(granted) => {
                    if granted {
                        granted_count += 1;
                        println!("RequestVote is granted.")
                    } else {
                        println!("RequestVote has not granted.")
                    }
                }
                Err(e) => println!("{:?}", e)
            }

            if self.network.is_majority(granted_count) {
                return true
            }
        }

        return false
    }

    fn send_request_vote(&self, node: &String, message: &[u8]) -> Result<bool, String> {
        match send_message(node, message) {
            Ok(res) => {
                let result = RequestVoteResult::from(&res);
                println!("RequestVoteResult: {:?}", result);
                Ok(result.vote_granted)
            }
            Err(e) => {
                Err(format!("Failed to read the RequestVoteResult: {:?}", e))
            }
        }
    }
}

struct Heartbeat {
    node_id: Arc<String>,
    network: Arc<Network>,
    state: Arc<RwLock<State>>,
    server_state: Arc<RwLock<ServerState>>,
}

impl Heartbeat {
    fn start(&self) {
        loop {
            // FIXME
            std::thread::sleep(std::time::Duration::from_millis(500));

            if self.server_state.read().unwrap().value != ServerStateValue::Leader {
                continue;
            }

            let message = RpcMessage::create_heartbeat(
                self.node_id.to_string(),
                &self.state.read().unwrap(),
            ).to_string();

            for node in self.network.nodes.iter() {
                self.send_heartbeat(&node, message.as_bytes());
            }
        }
    }

    fn send_heartbeat(&self, node: &String, message: &[u8]) -> Result<(), String> {
        match send_message(node, message) {
            Ok(res) => {
                let result = AppendEntriesResult::from(&res);
                println!("AppendEntriesResult: {:?}", result);
                Ok(())
            }
            Err(e) => {
                Err(format!("Failed to read the AppendEntriesResult: {:?}", e))
            }
        }
    }
}

fn send_message(node: &String, message: &[u8]) -> Result<Box<[u8]>, String> {
    match TcpStream::connect(format!("127.0.0.1:{}", node)) {
        Ok(mut stream) => {
            println!("Successfully connected to the node: {:?}", node);

            match stream.write(message) {
                Ok(size) => {
                    println!("Sent {} bytes", size);

                    let mut buffer = [0u8; 512];
                    match stream.read(&mut buffer) {
                        Ok(size) => {
                            Ok(buffer[..size].to_vec().into_boxed_slice())
                        }
                        Err(e) => {
                            Err(format!("Failed to read the response: {:?}", e))
                        }
                    }
                }
                Err(e) => {
                    Err(format!("Failed to RequestVote RPC: {:?}", e))
                }
            }
        }
        Err(e) => {
            Err(format!("Failed to connect to the node: {:?}, error: {:?}", node, e))
        }
    }
}