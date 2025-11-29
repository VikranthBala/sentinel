# Sentinel: Distributed Real-Time Fraud Detection Engine

![Status](https://img.shields.io/badge/Status-In%20Development-yellow)
![Stack](https://img.shields.io/badge/Stack-Kafka%20|%20Spark%20|%20Go%20|%20Postgres-blue)

## 📖 Overview
Sentinel is a real-time stream processing pipeline designed to detect fraudulent financial transactions. It ingests high-velocity transaction data, processes it using stateful windowing techniques, and flags anomalies based on user spending history.

## 🏗 Architecture
The system follows an Event-Driven Architecture (EDA):

1.  **Ingestion:** High-throughput JSON event producer written in **Golang**.
2.  **Buffering:** **Apache Kafka** decoupling producers from consumers.
3.  **Processing:** **Apache Spark Structured Streaming** (PySpark) handling windowed aggregations.
4.  **State Management:** **Redis** for low-latency lookups.
5.  **Storage:** **PostgreSQL** for persistent alert storage.
6.  **Visualization:** TUI Dashboard built with **Textual**.

## 🚀 Quick Start
*(Instructions to come...)*