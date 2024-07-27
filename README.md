# Katotonic

A leader-elected cluster to dole out monotonic IDs.

This daemon has 1 job, hand out IDs as fast as possible.

## What's inside

* Generates strictly monotonic IDs in ULID format
* Automatic cluster formation via [chitchat](https://github.com/quickwit-oss/chitchat)
* Leader election to ensure monotonicity
* Tokio-based server and client (default)
* Smol-based server and client (`default-features=false, features=["smol"]`)
* std::net based client

## Strictly monotonic

Uses a lock-free ID generator that ensures monotonicity via `fetch_update` on a `portable_atomic::AtomicU128`.  Using a Mutex as a barrier impacts performance dramatically in a negative way.

## How does it work

Every node gets assigned a random number when it starts. The node with the highest number becomes the leader upon joining or after a previous leader fails.
The client can connect to any node in the cluster, when the client requests an ID from a node that's not the leader the node will return the connection string for the leader.

## How does it perform

The async examples run 50 concurrent clients that are all generating a million IDs as fast as possible.  On my mini PC, this tops out between 550K - 700K IDs generated per second.

The sync example runs 5 concurrent threads, once we go over 30 this starts to be much more resource-hungry but does achieve a higher throughput than the async versions when a limited number of threads are working.

Due to differences in how locks are implemented, the rust components of this project perform a lot better on Linux than on my Macbook Pro M3.

