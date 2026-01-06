Benchmarking Performance of NoSQL Databases for E-Commerce
-----------------------------------------------------------------------------------------------------------------------
This repository contains the implementation, benchmarking scripts, datasets, and experimental results for a comparative performance evaluation of NoSQL databases in an e-commerce context.
The study benchmarks MongoDB, CouchDB, and Redis under identical workloads and hardware conditions to evaluate their suitability for modern e-commerce systems.

-----------------------------------------------------------------------------------------------------------------------

This project was completed as part of a Database Management Systems (DBMS) final assessment at Sunway University.
Subject: SEG2102 Database Management Systems
Institution: Sunway University
Year: 2024/2025
-----------------------------------------------------------------------------------------------------------------------
Authors
| Name                  | Student ID |
|-----------------------|------------|
| Ivan Sing Wen Sie     | 21063698   |
| Beatrice Chai Xin Xuan| 21060835   |
| Lai Yi Huey           | 21069620   |


School of Engineering and Technology
Sunway University, Malaysia
-----------------------------------------------------------------------------------------------------------------------
Project Objectives

The main objective of this project is to evaluate and compare the performance characteristics of three widely used NoSQL databases for e-commerce workloads:
MongoDB – Document-oriented database
CouchDB – Distributed document database
Redis – In-memory key-value store

The study focuses on answering:
Which NoSQL database provides lowest latency for user-facing operations?
Which database delivers highest throughput under transactional workloads?
How do these databases scale under concurrent access?
Which database is best suited for specific e-commerce tasks such as add-to-cart operations?

| Database | Type                  | Key Strength                             |
| -------- | --------------------- | ---------------------------------------- |
| MongoDB  | Document Store        | Balanced performance & flexible querying |
| CouchDB  | Document Store        | Replication & fault tolerance            |
| Redis    | Key-Value (In-Memory) | Ultra-low latency & high throughput      |

| Dataset                        | Purpose                  |
| ------------------------------ | ------------------------ |
| Online Retail II (Orders)      | Transaction processing   |
| Brazilian E-Commerce (Olist)   | Multi-vendor marketplace |
| E-Commerce Transactions        | Analytical queries       |
| Fashion Product Images (Small) | Product catalog data     |

-----------------------------------------------------------------------------------------------------------------------
Experimental Setup
-----------------------------------------------------------------------------------------------------------------------
Environment: Local single-node benchmarking
OS: Windows 10
CPU: AMD Ryzen 7 5700U
RAM: 8 GB
Storage: 512 GB SSD

To ensure fairness:
Only one database ran at a time
All databases used default configurations
Datasets and workloads were identical
Experiments were repeated and averaged
Performance Metrics

The following metrics were measured (SI units used throughout):
Latency (ms)
Read latency
Scan latency (range queries)
Insert latency
Update latency
Throughput (operations/second)
Single-threaded throughput
Multi-threaded scalability (up to 200 threads)
Application-Level Metric
Add-to-Cart throughput (combined read + write workflow)
Resource Usage
Memory consumption (MB)

-----------------------------------------------------------------------------------------------------------------------
| Workload    | Description                      |
| ----------- | -------------------------------- |
| Read        | Single record retrieval          |
| Scan        | Range / bulk reads               |
| Insert      | New record creation              |
| Update      | Record modification              |
| Add-to-Cart | Realistic e-commerce transaction |
-----------------------------------------------------------------------------------------------------------------------
## Setup and Installation

### Database Software
The following NoSQL databases were installed using official distributions:

- MongoDB (Community Edition)
- Apache CouchDB
- Redis

All databases were executed using default configurations without additional tuning.

### Dataset Preparation
Public e-commerce datasets were sourced from Kaggle and pre-processed
to ensure consistent schema representation across databases.

- Document databases (MongoDB, CouchDB): JSON format
- Key-value database (Redis): Hash-based structure

### Execution Notes
Only one database system was active at any time during benchmarking
to avoid resource contention. Each experiment was repeated and
average performance values were recorded.
