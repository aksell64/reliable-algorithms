# Reliable book algorithms

Implementation of algorithms from **"Introduction to Reliable and Secure Distributed Programming"** by Christian Cachin, Rachid Guerraoui, and Luís Rodrigues.

## About

This repository contains Go implementations of distributed programming abstractions and algorithms covered in the book. The goal is to provide clean, readable, and working code for each algorithm — from basic communication primitives to complex consensus protocols.

## Book Reference

**"Introduction to Reliable and Secure Distributed Programming"** (2nd Edition)

The book is a comprehensive guide to the fundamental abstractions and algorithms used in distributed systems. It covers failure models, communication primitives, consensus, and more.

## Topics Covered

### Basic Abstractions
- Perfect Links
- Best-Effort Broadcast
- Reliable Broadcast
- Uniform Reliable Broadcast

### Failure Detection
- Perfect Failure Detector
- Eventually Perfect Failure Detector

### Shared Memory
- Regular Registers
- Atomic Registers

### Consensus
- Flooding Consensus
- Hierarchical Consensus
- Uniform Consensus

### Total Order Broadcast
- Consensus-Based Total Order Broadcast

> **Note:** The list above reflects the scope of the book. Implementations are being added progressively.

## Tech Stack

- **Language:** Go
- **No external dependencies** — pure standard library where possible

## Getting Started

### Prerequisites

- Go 1.21 or higher

### Clone
```bash
git clone https://github.com/aksell64/relible.git
cd relible
```

### Run Tests
```bash
go test ./...
```

## Project Structure
```
relible/
├── README.md
├── go.mod
└── ...          # packages organized by abstraction layer
```

## Motivation

The best way to understand distributed algorithms is to implement them. This project is a hands-on companion to the book, translating pseudocode into real, runnable Go code.

## Contributing

Contributions, suggestions, and discussions are welcome! Feel free to open an issue or submit a pull request.

## License

This project is open source. See the [LICENSE](LICENSE) file for details.

## References

- Cachin, C., Guerraoui, R., & Rodrigues, L. (2011). *Introduction to Reliable and Secure Distributed Programming* (2nd ed.). Springer.