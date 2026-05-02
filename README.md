# Real-Time Stream Processing with Apache Storm and Docker

## Overview

This tutorial walks you through running a real-time stream processing topology on Apache Storm using Docker. The topology consumes a live stream of Wikipedia edits via Wikimedia EventStreams (Server-Sent Events) and processes them through a pipeline of spouts and bolts to find the most frequently occurring keywords.

### Architecture

The topology has four stages:

1. **WikimediaStreamSpout** — connects to `stream.wikimedia.org` via SSE and emits live English Wikipedia edits as tuples.
2. **SplitSentenceBolt** — splits each edit into words and filters for a predefined set of keywords.
3. **WordCountBolt** — maintains a running count for each matched keyword.
4. **TopNFinderBolt** — tracks and reports the top N most frequent keywords.

The Storm cluster runs in Docker with three containers:

- **Zookeeper** — coordination service for the cluster.
- **Nimbus** — the master node that distributes topologies to workers.
- **Supervisor** — the worker node that runs spout and bolt instances across multiple JVM processes.


## Prerequisites

- Docker and Docker Compose
- Java 17 (Storm 2.8.7 requires it)
- Maven 3.8+ (older versions are incompatible with Java 17)

Verify your setup:

```bash
docker --version
java -version     # should show version 17.x
mvn --version     # should show 3.8+ and Java 17
```

## Step 1 — Clone the Repository

```bash
git clone git@github.com:przybylek/Apache_Storm_Demo.git
cd Apache_Storm_Demo
```


## Step 2 — Start the Storm Cluster

```bash
docker compose up -d
```

Initial startup takes 1–2 minutes while Storm initializes. Verify all three containers are running:

```bash
docker compose ps
```

All containers (`zookeeper`, `nimbus`, `supervisor`) should show a status of "Up". If any show "Restarting", wait a moment — Nimbus waits for Zookeeper, and Supervisor waits for both.


## Step 3 — Build the Topology

```bash
cd WikimediaStorm
mvn clean package -DskipTests
```

This compiles the Java sources and produces a fat JAR at `target/storm-example-0.0.1-SNAPSHOT-jar-with-dependencies.jar`.


## Step 4 — Deploy the Topology

Copy the JAR into the Nimbus container and submit it:

```bash
docker cp target/storm-example-0.0.1-SNAPSHOT-jar-with-dependencies.jar nimbus:/tmp/topology.jar
docker exec nimbus storm jar /tmp/topology.jar TwitterTopology remote
```

The `remote` argument tells the topology to submit to the distributed cluster (as opposed to running locally).


## Step 5 — Verify the Topology is Running

```bash
docker exec nimbus storm list
```

You should see `twitter-word-count` listed with status `ACTIVE`, along with the number of tasks, workers, and uptime.


## Step 6 — Inspect Worker Logs

Storm distributes the topology across multiple worker processes. Each worker writes to its own log file. To list all worker logs:

```bash
docker exec supervisor find /logs -name "worker.log" 2>/dev/null
```

You will see multiple files like:

```
/logs/workers-artifacts/twitter-word-count-1-.../6700/worker.log
/logs/workers-artifacts/twitter-word-count-1-.../6701/worker.log
/logs/workers-artifacts/twitter-word-count-1-.../6702/worker.log
```

The numbers `6700`, `6701`, `6702` are worker ports. Each worker JVM runs a different subset of the topology's spouts and bolts, so their logs contain different output.

### Viewing stream connection status

The spout logs its connection to Wikimedia EventStreams:

```bash
docker exec supervisor bash -c 'grep WIKI /logs/workers-artifacts/twitter-word-count-*/*/worker.log' | tail -n 10
```

You should see messages like `[WIKI] SSE connected!` followed by `[WIKI] Edit #1: ...`, `[WIKI] Edit #2: ...`, confirming live data is flowing.

### Viewing the Top-N results

The `TopNFinderBolt` periodically reports the most frequent keywords:

```bash
docker exec supervisor bash -c 'grep "top-words" /logs/workers-artifacts/twitter-word-count-*/*/worker.log' | tail -n 20
```

### Viewing output from each stage

Each processing stage logs with a distinct prefix (`[SPLIT]`, `[COUNT]`, `[TOP-N]`), making it easy to filter:

```bash
# Keywords matched by SplitSentenceBolt
docker exec supervisor bash -c 'grep SPLIT /logs/workers-artifacts/twitter-word-count-*/*/worker.log' | tail -n 10

# Running word counts
docker exec supervisor bash -c 'grep COUNT /logs/workers-artifacts/twitter-word-count-*/*/worker.log' | tail -n 10

# Top-N reports
docker exec supervisor bash -c 'grep TOP-N /logs/workers-artifacts/twitter-word-count-*/*/worker.log' | tail -n 10
```


## Step 7 — Stop and Clean Up

Kill the running topology:

```bash
docker exec nimbus storm kill twitter-word-count
```

Wait about 10 seconds for workers to shut down gracefully. Then clean up old log files:

```bash
docker exec supervisor bash -c 'rm -rf /logs/workers-artifacts/twitter-word-count-*'
```

To shut down the entire cluster:

```bash
docker compose down
```


## Redeploying After Code Changes

If you modify any Java source file:

```bash
docker exec supervisor bash -c 'rm -rf /logs/workers-artifacts/twitter-word-count-*'
docker exec nimbus storm kill twitter-word-count
mvn clean package -DskipTests
sleep 20
docker exec nimbus storm list
docker cp target/storm-example-0.0.1-SNAPSHOT-jar-with-dependencies.jar nimbus:/tmp/topology.jar
docker exec nimbus storm jar /tmp/topology.jar TwitterTopology remote
```
