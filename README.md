# Reliable book algorithms

Implementation of algorithms from **"Introduction to Reliable and Secure Distributed Programming"** by Christian Cachin, Rachid Guerraoui, and Luís Rodrigues.

## About

This repository contains Go implementations of distributed programming abstractions and algorithms covered in the book. The goal is to provide clean, readable, and working code for each algorithm — from basic communication primitives to complex consensus protocols.

## Book Reference

**"Introduction to Reliable and Secure Distributed Programming"** (2nd Edition)

The book is a comprehensive guide to the fundamental abstractions and algorithms used in distributed systems. It covers failure models, communication primitives, consensus, and more.

## Topics Covered

### Basic Abstractions
- Perfect, Stubborn Links (in-memory, libp2p)
- Best-Effort Broadcast
- Reliable Broadcast
- Byzantine Broadcast
- Byzantine Channel

### Election

- Lower Epoch Election
- Monarchical Election
- Byzantine Rotating Election

### Failure Detection
- Perfect Failure Detector
- Eventually Perfect Failure Detector


### Consensus

#### Fail-Silent

- Randomized Consensus

#### Fail-Noisy

- Leader-Based Consensus
- Byzantine Leader-Based Consensus

#### Common-Coin

- Commit Reveal Common Coin
- TBLS Common Coin (In the future)

### DKG

- Feldman-Peterson DKG

> **Note:** The list above reflects the scope of the book. Implementations are being added progressively.


## Motivation

The best way to understand distributed algorithms is to implement them. This project is a hands-on companion to the book, translating pseudocode into real, runnable Go code.



## References

- Cachin, C., Guerraoui, R., & Rodrigues, L. (2011). *Introduction to Reliable and Secure Distributed Programming* (2nd ed.). Springer.