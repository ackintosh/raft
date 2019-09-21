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
