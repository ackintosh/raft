# Raft

https://raft.github.io/raft.pdf

## Usage

- Node1 (to be elected leader)
```bash
$ cargo run 8080 8081 8082
```

- Node2
```bash
$ cargo run 8081 8082 8080
```

- Node3
```bash
$ cargo run 8082 8081 8080
```

- Client
```bash
# Send a command to the leader
$ telnet 127.0.0.1 8080
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
{"type":"StateMachineCommand","payload":"sample command"}
OK
Connection closed by foreign host.
```
