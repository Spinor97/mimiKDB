# fook-kdb

A lightweight, high-performance, column-oriented time-series database engine written in **Rust**. Built to mimic core **kdb+** functionality, it supports high-throughput ingestion of **FIX 4.4 protocol** messages via a hybrid storage layer that balances an in-memory Real-Time Database (RDB) cache with a disk-serialized Historical Database (HDB).

## Features

- **kdb+ Mimicry:** Column-oriented, time-partitioned vector layouts designed for high-performance financial data operations.
- **Hybrid Storage Layer:** 
  - **In-Memory Cache (RDB):** Ingests and structures real-time incoming trading streams for instant query accessibility.
  - **Auto-Flush Persistence (HDB):** Automatically flushes data to local, immutable disk storage when memory/cache thresholds are met.
- **FIX 4.4 Protocol Engine:** Parses, validates, and processes incoming financial data streams mapped via structured XML configuration specifications.
- **Memory & Thread Safety:** Engineered entirely in Rust, maximizing throughput with predictable memory layouts and zero-cost concurrency abstractions.

## Project Structure

```text
├── Cargo.toml         # Rust dependency and package management
├── docs/              # System architecture specs and documentation
├── fook_kdb/          # Main application executable engine
├── lib/               # Modular database engine, column encoders, and network libraries
└── superpowers/      # Optimization layers and core algorithmic specs
```

## Getting Started

### Prerequisites

- **Rust toolchain** (Stable or Nightly)

### Installation

Clone the repository and build the project using Cargo:

```bash
git clone https://github.com
cd fook-kdb
cargo build --release
```

## Architecture Overview

1. **Ingestion Layer:** Reads FIX 4.4 financial streams.
2. **Real-Time Database (RDB):** Caches incoming column vectors sequentially in RAM.
3. **Flushing Monitor:** Tracks memory limits and cache footprints.
4. **Historical Database (HDB):** serializes blocks down to local disk partition pathways as immutable binary arrays when cache limits trip.

## License

This project is open-source. For distribution or contribution inquiries, please review the repository settings.
