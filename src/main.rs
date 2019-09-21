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
    let volatile_state = Arc::new(RwLock::new(VolatileState::new()));

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
        volatile_state: volatile_state.clone(),
    };
    let _heartbeat_handle = std::thread::spawn(move || {
        heartbeat.start();
    });

    let mut rpc_handler = RpcHandler {
        node_id: node_id.clone(),
        state: state.clone(),
        server_state: server_state.clone(),
        volatile_state: volatile_state.clone(),
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

    fn eq_majority(&self, i: i32) -> bool {
        i == self.majority
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
    logs: Vec<Log>,
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
        println!("currentTerm has been increased from {} to {}", self.current_term, self.current_term + 1);
        self.current_term += 1;
    }

    fn update_term(&mut self, term: u64) {
        println!("currentTerm has been updated from {} to {}", self.current_term, term);
        self.current_term = term;
    }

    fn voted_for(&mut self, node_id: &String) {
        self.voted_for = Some(node_id.to_string())
    }

    fn append_log(&mut self, log: Vec<Log>) {
        println!("logs has been appended with the log: {:?}", log);
        for l in log.iter() {
            // Receiver implementation:
            // 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
            // 4. Append any new entries not already in the log
            if let Some(conflicted) = self.log_conflicts(&l) {
                let pos = self.logs.iter().position(|l| {
                    l.index == conflicted.index && l.term == conflicted.term
                });

                match pos {
                    Some(pos) => {
                        println!("The log has been replaced. from: {:?}, to: {:?}", conflicted, l);
                        self.logs.remove(pos);
                        self.logs.insert(pos, l.clone());
                    }
                    None => {
                        // TODO
                        panic!("Failed to replace_log: couldn't find the `conflicted` Log");
                    }
                }
            } else {
                assert_eq!(l.index, (self.log_index() + 1));
                self.logs.push(l.clone());
            }
        }

        println!("state.logs: {:?}", self.logs);
    }

    fn log_index(&self) -> u64 {
        if self.logs.is_empty() {
            return 0
        }
        self.logs.last().unwrap().index
    }

    fn prev_log(&self) -> Option<&Log> {
        let prev_idx = self.logs.len().checked_sub(2)?;
        self.logs.get(prev_idx)
    }

    fn log_term(&self) -> u64 {
        if self.logs.is_empty() {
            0
        } else {
            self.logs.last().unwrap().term
        }
    }

    fn has_log(&self, term: u64, index: u64) -> bool {
        self.logs.iter().any(|l| {
            l.term == term && l.index == index
        })
    }

    fn log_conflicts(&self, log: &Log) -> Option<&Log> {
        self.logs.iter().find(|l| {
            l.index == log.index && l.term != log.term
        })
    }
}

// Volatile state on all servers
struct VolatileState {
    // index of highest log entry known to be committed (initialized to 0, increases monotonically)
    commit_index: u64,
    // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    last_applied: u64,
}

impl VolatileState {
    fn new() -> Self {
        Self {
            commit_index: 0,
            last_applied: 0,
        }
    }

    fn increment_commit_index(&mut self) -> u64 {
        println!("commitIndex has been incremented from {} to {}", self.commit_index, self.commit_index + 1);
        self.commit_index += 1;
        self.commit_index
    }

    fn update_commit_index(&mut self, i: u64) {
        assert!(i > self.commit_index);
        println!("commitIndex has been updated from {} to {}", self.commit_index, i);
        self.commit_index = i;
    }

    fn update_last_applied(&mut self, log: &Log) {
        assert_eq!(log.index, self.last_applied + 1);
        println!("lastApplied has been updated from {} to {}", self.last_applied, log.index);
        self.last_applied = log.index;
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Log {
    term: u64,
    index: u64,
    command: String,
}

struct RpcHandler {
    node_id: Arc<String>,
    state: Arc<RwLock<State>>,
    server_state: Arc<RwLock<ServerState>>,
    volatile_state: Arc<RwLock<VolatileState>>,
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
            RpcMessageType::StateMachineCommand => self.handle_state_machine_command(stream, &message),
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

            // Rules for Servers:
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            if request_vote.term > state.current_term {
                state.update_term(request_vote.term);
                self.server_state.write().unwrap().to_follower();
            }

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
                        && request_vote.last_log_term >= state.log_term()
                }
            }
            None => {
                request_vote.last_log_index >= state.log_index()
                    && request_vote.last_log_term >= state.log_term()
            }
        }
    }

    fn handle_append_entries(&self, mut strem: &TcpStream, message: &RpcMessage) {
        let append_entries = AppendEntries::from(&message.payload);

        let is_valid = self.verify_append_entries(&append_entries);

        let mut state = self.state.write().unwrap();

        if is_valid {
            // Rules for Servers:
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            if append_entries.term > state.current_term {
                state.update_term(append_entries.term);
                self.server_state.write().unwrap().to_follower();
            }

            // Receiver implementation:
            // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of the last new entry)
            {
                let commit_index = self.volatile_state.read().unwrap().commit_index;
                if append_entries.leader_commit > commit_index {
                    self.volatile_state.write().unwrap().update_commit_index(
                        append_entries.leader_commit.min(
                            append_entries.entries.last().unwrap().index
                        )
                    );
                }
            }

            // empty for heartbeat
            if !append_entries.entries.is_empty() {
                state.append_log(append_entries.entries.clone());

                // NOTE:
                // Suppose the follower applies the command to its state machine like below:
                // state_machine.apply(append_entries.entries)

                self.volatile_state.write().unwrap().update_last_applied(append_entries.entries.last().unwrap());
            }

            self.heartbeat_received_at.write().unwrap().reset();
        }

        let result = serde_json::to_string(&AppendEntriesResult {
            term: state.current_term,
            success: is_valid,
        }).unwrap();

        println!("AppendEntriesResult: {:?}", result);
        strem.write(result.as_bytes()).unwrap();
    }

    fn verify_append_entries(&self, append_entries: &AppendEntries) -> bool {
        let state = self.state.read().unwrap();
        // Receiver implementation:
        // 1. Reply false if term < currentTerm (§5.1)
        if append_entries.term < state.current_term {
            return false
        }

        // Heartbeat immediately after an initial election
        if append_entries.prev_log_term == 0 || append_entries.prev_log_index == 0 {
            return true
        }

        // Receiver implementation:
        // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
        if !state.has_log(append_entries.prev_log_term, append_entries.prev_log_index) {
            return false
        }

        true
    }

    fn handle_state_machine_command(&self, mut stream: &TcpStream, message: &RpcMessage) {
        if self.server_state.read().unwrap().value != ServerStateValue::Leader {
            let rep = "Received StateMachineCommand but leader is other node in currentTerm";
            println!("{}", rep);
            stream.write(rep.as_bytes()).unwrap();
            return;
        }

        // If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
        let mut state = self.state.write().unwrap();
        let mut volatile_state = self.volatile_state.write().unwrap();

        let log = Log {
            term: state.current_term,
            index: volatile_state.increment_commit_index(),
            command: message.payload.clone()
        };
        state.append_log(vec![log.clone()]);

        let message = RpcMessage::create_append_entries(
            self.node_id.to_string(),
            &state,
            &volatile_state,
            log.clone()
        ).to_string();

        // Send AppendEntries RPC in parallel
        let mut handles = vec![];
        for node in self.network.nodes.iter() {
            let n = node.clone();
            let m = message.clone();
            handles.push(
                std::thread::spawn(move || {
                    Self::send_append_entries(&n, m.as_bytes());
                })
            )
        }

        // When the entry has been safely replicated, the leader applies the entry to its state machine and returns the result of that execution to the client.
        let mut replicated_count = 0;
        for h in handles {
            h.join();
            replicated_count += 1;

            // A log entry is committed once the leader that created the entry has replicated it on a majority of the servers.
            if self.network.eq_majority(replicated_count) {
                // NOTE:
                // Suppose the leader applies the command to its state machine like below:
                // state_machine.apply(log.command)

                volatile_state.update_last_applied(&log);
                stream.write("OK\n".as_bytes()).unwrap();
            }
        }

    }

    fn send_append_entries(node: &String, message: &[u8]) -> Result<(), String>{
        match send_message(node, message) {
            Ok(res) => {
                // TODO:
                // 5.3 Log replication
                // If followers crash or run slowly, or if network packets are lost,
                // the leader retries AppendEntries RPCs indefinitely (even after it has responded to the client)
                // until all followers eventually store all log entries.
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

    fn create_heartbeat(
        node_id: String,
        state: &RwLockReadGuard<State>,
        volatile_state: &RwLockReadGuard<VolatileState>
    ) -> Self {
        let payload = serde_json::to_string(
            &AppendEntries::new(
                node_id,
                state.current_term,
                state.log_index(),
                state.log_term(),
                volatile_state.commit_index,
                vec![]
            )
        ).unwrap();

        Self {
            r#type: RpcMessageType::AppendEntries,
            payload,
        }
    }

    fn create_append_entries(
        node_id: String,
        state: &RwLockWriteGuard<State>,
        volatile_state: &RwLockWriteGuard<VolatileState>,
        log: Log
    ) -> Self {
        let (prev_log_index, prev_log_term) =
            if let Some(prev_log) = state.prev_log() {
                (prev_log.index, prev_log.term)
            } else {
                (0, 0)
            };

        let payload = serde_json::to_string(
            &AppendEntries::new(
                node_id,
                state.current_term,
                prev_log_index,
                prev_log_term,
                volatile_state.commit_index,
                vec![log]
            )
        ).unwrap();

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
    // Invoked by client to apply a command.
    StateMachineCommand,
}

#[derive(Debug, Serialize, Deserialize)]
struct RequestVote {
    // candidate's term
    term: u64,
    // candidate requesting vote
    candidate_id: String,
    // index of candidate's last log entry (§5.4)
    last_log_index: u64,
    // term of candidate's log entry (§5.4)
    last_log_term: u64,
}

impl RequestVote {
    fn new(node_id: String, state: &RwLockWriteGuard<State>) -> Self {
        Self {
            term: state.current_term,
            candidate_id: node_id.to_string(),
            last_log_index: state.log_index(),
            last_log_term: state.log_term(),
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
    entries: Vec<Log>,
    // leader's commitIndex
    leader_commit: u64,
}

impl AppendEntries {
    fn new(
        node_id: String,
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        leader_commit: u64,
        entries: Vec<Log>
    ) -> Self {
        Self {
            term,
            leader_id: node_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
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

struct LeaderElection {
    node_id: Arc<String>,
    network: Arc<Network>,
    state: Arc<RwLock<State>>,
    server_state: Arc<RwLock<ServerState>>,
    election_timeout: Duration,
    heartbeat_received_at: Arc<RwLock<HeartbeatReceivedAt>>,
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
                // Rules for Servers:
                // Followers (§5.2):
                // If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
                println!("Receives no communication over a period `election timeout`.");
                self.server_state.write().unwrap().to_candidate();

                if self.start_election() {
                    // If votes received from majority of servers: become leader
                    println!("Received votes from a majority of the servers in the full cluster for the same term.");
                    self.server_state.write().unwrap().to_leader();
                } else {
                    // NOTE:
                    // Change its state to Follower in order to wait for heartbeat(AppendEntries with empty entries) from a new leader.
                    // If no communication from the new leader, a new election will be started.
                    self.server_state.write().unwrap().to_follower();
                }

                println!("Reset the heartbeat_received_at");
                self.heartbeat_received_at.write().unwrap().reset();
            } else {
                std::thread::sleep(timeout - now);
            }
        }
    }

    fn start_election(&self) -> bool {
        // Rules for Servers:
        // Candidates (§5.2):
        // On conversion to candidate, start election:
        // * Increment currentTerm
        // * Vote for self
        // * Reset election timer
        // * Send RequestVote RPCs to all other servers
        println!("The election has been started");

        // To begin an election, a follower increments its current term and transitions to candidate state.
        let mut state = self.state.write().unwrap();
        state.increment_term();
        state.voted_for(&self.node_id);

        let message = RpcMessage::create_request_vote(
            self.node_id.to_string(),
            &state
        ).to_string();


        // Send RequestVote RPC in parallel
        let mut handles = vec![];
        for node in self.network.nodes.iter() {
            let n = node.clone();
            let m = message.clone();
            handles.push(
                std::thread::spawn(move || {
                    match Self::send_request_vote(n, m.as_bytes()) {
                        Ok(granted) => granted,
                        Err(e) => {
                            println!("{}", e);
                            false
                        }
                    }
                })
            );
        }

        let mut granted_count = 0;
        for h in handles {
            if h.join().unwrap_or(false) {
                granted_count += 1;
                if self.network.is_majority(granted_count) {
                    return true
                }
            }
        }

        return false
    }

    fn send_request_vote(node: String, message: &[u8]) -> Result<bool, String> {
        match send_message(&node, message) {
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
    volatile_state: Arc<RwLock<VolatileState>>,
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
                &self.volatile_state.read().unwrap()
            ).to_string();

            for node in self.network.nodes.iter() {
                let n = node.clone();
                let m = message.clone();
                // NOTE:
                // Detach the threads to avoid cascading issues due to crashed or slow followers
                std::thread::spawn(move || {
                    match Self::send_heartbeat(n, m.as_bytes()) {
                        Ok(_) => {},
                        Err(e) => println!("{}", e)
                    }
                });
            }
        }
    }

    fn send_heartbeat(node: String, message: &[u8]) -> Result<(), String> {
        match send_message(&node, message) {
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