# Paul-H Key Value Store #

This is a en educationnal project based on the Talent-Plan Rust course from Pingcap. 
A simple Key-Value store based on few premises : 
* We only store text, the engine can not store anything else for the value.
* Storage is organised as described by the bitcask paper 

## Non goals ##
* Produe another key-value store for the market
* Produe a high performance, reliability, whatever DB engine
* Produce a production-ready software 

## Goals ##
* Learn more about Rust and tacke a real-world project
* Learn more about database storage
* Learn more about distributed-systems (The talent Rust course is the entry gate for their distributed systems course with Rust :) )

## Future plans ##
* Finish the PingCap course (Add multithreading to the server and asynchronous IOs)
* Implement a REPL with basics instructions as "INSERT Key1 Val1" or "GET values WHERE Key <> 'test'". The goal would be to learn more about lexers
* Do some benchmarks to try and see the impact of modifications in the code

