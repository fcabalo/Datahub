# Datahub POC Use Cases

This document describes the test use cases for the Datahub POC system, including routing matrices and expected behavior.

---

## Overview

The Datahub POC system routes messages from producers through Apache Kafka to consumers based on configurable routing criteria. Messages are transformed and routed based on message type, source, destination, and region.

---

## System Components

### Partners

| Partner ID | Partner Name | Region | Description |
|------------|--------------|--------|-------------|
| 1 | Default | Berlin | Dead-letter/fallback partner |
| 2 | AC Route | Hesse | Main routing partner with type-based rules |
| 3 | SPT | Berlin | Region-based routing partner |
| 4 | Live Operations | Hesse | Currently inactive |

### Partner Interfaces

| Interface ID | Topic Name | Partner ID | Partner Name | Direction | Type | Region | Status |
|--------------|------------|------------|--------------|-----------|------|--------|--------|
| 1 | PI1Incoming | 1 | Default | INCOMING | DEFAULT | Berlin | LIVE |
| 2 | PI2Outgoing | 1 | Default | OUTGOING | DEFAULT | Berlin | LIVE |
| 3 | PI3Incoming | 2 | AC Route | INCOMING | RESTAPI | Hesse | LIVE |
| 4 | PI4Outgoing | 2 | AC Route | OUTGOING | TCPIP | Hesse | LIVE |
| 5 | PI5Outgoing | 2 | AC Route | OUTGOING | TCPIP | Hesse | LIVE |
| 6 | PI6Incoming | 3 | SPT | INCOMING | RESTAPI | Berlin | LIVE |
| 7 | PI7Outgoing | 3 | SPT | OUTGOING | TCPIP | Berlin | LIVE |
| 8 | PI8Outgoing | 4 | Live Operations | OUTGOING | TCPIP | Hesse | INACTIVE |

### Routing Criteria

| RC ID | Target Interface | Target Topic | Condition | Active |
|-------|------------------|--------------|-----------|--------|
| RC1 | 4 | PI4Outgoing | partnerId=2 AND messageType="A" | YES |
| RC2 | 5 | PI5Outgoing | partnerId=2 AND messageType NOT "A" | YES |
| RC3 | 7 | PI7Outgoing | region="Hesse" | YES |
| RC4 | 8 | PI8Outgoing | messageType IN ["A","B"] | NO (INACTIVE) |
| Dead Letter | 2 | PI2Outgoing | No criteria match | YES |

---

## Use Case 1: Broadcast (Few Messages to Many Partners)

### Description

Tests message fan-out behavior where a small number of messages are routed to multiple partners based on different routing criteria.

### Command

```bash
python3 use_case_1.py <message_count>
```

### Arguments

- message_count: Number of messages to send (required)

### Examples

```bash
# Send 10 messages cycling through 5 routing patterns
python3 use_case_1.py 10

# Send 25 messages (5 complete cycles)
python3 use_case_1.py 25
```

### Message Patterns

| Pattern | Source | Type | Destination | Routes To | Partners Receiving |
|---------|--------|------|-------------|-----------|-------------------|
| 1 | 3 | A | 2 | PI4Outgoing, PI7Outgoing | Partner 2, Partner 3 |
| 2 | 3 | B | 2 | PI5Outgoing, PI7Outgoing | Partner 2, Partner 3 |
| 3 | 3 | C | 2 | PI5Outgoing, PI7Outgoing | Partner 2, Partner 3 |
| 4 | 3 | A | null | PI7Outgoing | Partner 3 |
| 5 | 6 | A | 2 | PI4Outgoing | Partner 2 |

### Routing Matrix for 10 Messages

| Message | Pattern | Source | Type | Dest | Region | PI2 | PI4 | PI5 | PI7 | Partner 1 | Partner 2 | Partner 3 |
|---------|---------|--------|------|------|--------|-----|-----|-----|-----|-----------|-----------|-----------|
| 1 | 1 | 3 | A | 2 | Hesse | NO | YES | NO | YES | NO | YES | YES |
| 2 | 2 | 3 | B | 2 | Hesse | NO | NO | YES | YES | NO | YES | YES |
| 3 | 3 | 3 | C | 2 | Hesse | NO | NO | YES | YES | NO | YES | YES |
| 4 | 4 | 3 | A | null | Hesse | NO | NO | NO | YES | NO | NO | YES |
| 5 | 5 | 6 | A | 2 | Berlin | NO | YES | NO | NO | NO | YES | NO |
| 6 | 1 | 3 | A | 2 | Hesse | NO | YES | NO | YES | NO | YES | YES |
| 7 | 2 | 3 | B | 2 | Hesse | NO | NO | YES | YES | NO | YES | YES |
| 8 | 3 | 3 | C | 2 | Hesse | NO | NO | YES | YES | NO | YES | YES |
| 9 | 4 | 3 | A | null | Hesse | NO | NO | NO | YES | NO | NO | YES |
| 10 | 5 | 6 | A | 2 | Berlin | NO | YES | NO | NO | NO | YES | NO |

### Expected Results

| Metric | Value |
|--------|-------|
| Messages Sent | 10 |
| Total Deliveries | 16 (fan-out) |
| Partner 1 Receives | 0 messages |
| Partner 2 Receives | 8 messages (4 via PI4, 4 via PI5) |
| Partner 3 Receives | 8 messages (all via PI7) |

### Consumer Setup

```bash
# Terminal 1: Consumer for Partner 2
python3 consumer.py 2 9293 10

# Terminal 2: Consumer for Partner 3
python3 consumer.py 3 9293 10

# Terminal 3: Run test
python3 use_case_1.py 10
```

---

## Use Case 2: Load Test (Many Messages to One Partner)

### Description

Tests high-volume message throughput and performance by sending many messages to a single partner or testing fan-out behavior under load.

### Command

```bash
python3 use_case_2.py <message_count> [source] [messageType] [destination] [batch_size]
```

### Arguments

| Argument | Required | Default | Options | Description |
|----------|----------|---------|---------|-------------|
| message_count | YES | - | Any positive integer | Number of messages to send |
| source | NO | 6 | 3 or 6 | Source interface (3=Hesse, 6=Berlin) |
| messageType | NO | A | A, B, or C | Message type |
| destination | NO | 2 | 1, 2, 3, or 4 | Target partner |
| batch_size | NO | 100 | Any positive integer | Progress reporting interval |

### Examples

#### Example 1: Default (No Fan-out)

```bash
python3 use_case_2.py 1000
```

**Configuration:**
- 1000 messages
- Source: 6 (Berlin)
- Type: A
- Destination: 2

**Routing:**

| Messages | Source | Type | Dest | Region | Routes To | Partners |
|----------|--------|------|------|--------|-----------|----------|
| 1000 | 6 | A | 2 | Berlin | PI4Outgoing | Partner 2 only |

**Expected Results:**
- Partner 2 receives: 1000 messages (via PI4)
- Partner 3 receives: 0 messages
- Total deliveries: 1000

#### Example 2: With Fan-out

```bash
python3 use_case_2.py 1000 3 A 2 100
```

**Configuration:**
- 1000 messages
- Source: 3 (Hesse)
- Type: A
- Destination: 2
- Progress every 100 messages

**Routing:**

| Messages | Source | Type | Dest | Region | Routes To | Partners |
|----------|--------|------|------|--------|-----------|----------|
| 1000 | 3 | A | 2 | Hesse | PI4Outgoing, PI7Outgoing | Partner 2, Partner 3 |

**Expected Results:**
- Partner 2 receives: 1000 messages (via PI4)
- Partner 3 receives: 1000 messages (via PI7)
- Total deliveries: 2000 (fan-out)

#### Example 3: Type B Messages

```bash
python3 use_case_2.py 5000 6 B 2 500
```

**Configuration:**
- 5000 messages
- Source: 6 (Berlin)
- Type: B
- Destination: 2
- Progress every 500 messages

**Routing:**

| Messages | Source | Type | Dest | Region | Routes To | Partners |
|----------|--------|------|------|--------|-----------|----------|
| 5000 | 6 | B | 2 | Berlin | PI5Outgoing | Partner 2 only |

**Expected Results:**
- Partner 2 receives: 5000 messages (via PI5)
- Partner 3 receives: 0 messages
- Total deliveries: 5000

### Source Parameter Impact

| Source | Region | RC3 Match | Result |
|--------|--------|-----------|--------|
| 6 | Berlin | NO | No fan-out to Partner 3 |
| 3 | Hesse | YES | Fan-out to Partner 3 |

### Consumer Setup

#### No Fan-out Test

```bash
# Terminal 1: Consumer for Partner 2
python3 consumer.py 2 9293 1000

# Terminal 2: Send messages
python3 use_case_2.py 1000 6 A 2 100
```

#### Fan-out Test

```bash
# Terminal 1: Consumer for Partner 2
python3 consumer.py 2 9293 1000

# Terminal 2: Consumer for Partner 3
python3 consumer.py 3 9293 1000

# Terminal 3: Send messages
python3 use_case_2.py 1000 3 A 2 100
```

---

## Complete Routing Matrix

### By Message Type and Source

#### Type A Messages

| Source | Destination | Region | Routes To | Partners | Consumer Commands |
|--------|-------------|--------|-----------|----------|-------------------|
| 3 | 2 | Hesse | PI4, PI7 | 2, 3 | PARTNER_ID=2, PARTNER_ID=3 |
| 3 | null | Hesse | PI7 | 3 | PARTNER_ID=3 |
| 6 | 2 | Berlin | PI4 | 2 | PARTNER_ID=2 |
| 6 | null | Berlin | PI2 | 1 | PARTNER_ID=1 |

#### Type B Messages

| Source | Destination | Region | Routes To | Partners | Consumer Commands |
|--------|-------------|--------|-----------|----------|-------------------|
| 3 | 2 | Hesse | PI5, PI7 | 2, 3 | PARTNER_ID=2, PARTNER_ID=3 |
| 3 | null | Hesse | PI7 | 3 | PARTNER_ID=3 |
| 6 | 2 | Berlin | PI5 | 2 | PARTNER_ID=2 |
| 6 | null | Berlin | PI2 | 1 | PARTNER_ID=1 |

#### Type C Messages

| Source | Destination | Region | Routes To | Partners | Consumer Commands |
|--------|-------------|--------|-----------|----------|-------------------|
| 3 | 2 | Hesse | PI5, PI7 | 2, 3 | PARTNER_ID=2, PARTNER_ID=3 |
| 3 | null | Hesse | PI7 | 3 | PARTNER_ID=3 |
| 6 | 2 | Berlin | PI5 | 2 | PARTNER_ID=2 |
| 6 | null | Berlin | PI2 | 1 | PARTNER_ID=1 |

---

## Consumer Commands Reference

### By Partner

| Partner | Partner ID | Receives From | Consumer Command |
|---------|-----------|---------------|------------------|
| Default | 1 | PI2Outgoing | python3 consumer.py 1 9293 COUNT |
| AC Route | 2 | PI4Outgoing + PI5Outgoing | python3 consumer.py 2 9293 COUNT |
| SPT | 3 | PI7Outgoing | python3 consumer.py 3 9293 COUNT |
| Live Operations | 4 | PI8Outgoing (INACTIVE) | python3 consumer.py 4 9293 COUNT |

### Consumer Arguments

```bash
python3 consumer.py <partnerId> <port> <messageCount>
```

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| partnerId | YES | - | Partner ID (1, 2, 3, or 4) |
| port | NO | 9293 | TCP port for Adapter |
| messageCount | NO | 0 | Number of messages to receive (0 = continuous) |

---

## Kafka Topics

### Incoming Topics

| Topic | Source | Purpose | Consumer |
|-------|--------|---------|----------|
| PI1Incoming | Interface 1 | Partner 1 incoming | StreamProcessor |
| PI3Incoming | Interface 3 | Partner 2 incoming | StreamProcessor |
| PI6Incoming | Interface 6 | Partner 3 incoming | StreamProcessor |
| IncomingTopic | All sources | Central processing | StreamProcessor |

### Outgoing Topics

| Topic | Target Partner | Purpose | Consumer |
|-------|---------------|---------|----------|
| PI2Outgoing | Partner 1 | Dead letter | Adapter (Partner 1) |
| PI4Outgoing | Partner 2 | Type A messages | Adapter (Partner 2) |
| PI5Outgoing | Partner 2 | Type B/C messages | Adapter (Partner 2) |
| PI7Outgoing | Partner 3 | Hesse region messages | Adapter (Partner 3) |
| PI8Outgoing | Partner 4 | INACTIVE | None |

---

## Prerequisites

Before running tests, ensure all services are running:

```bash
# 1. Start Kafka
docker-compose -f Docker/docker-kafka.yml up

# 2. Start DatahubPOC (in datahubPOC directory)
./mvnw spring-boot:run

# 3. Start Adapter (in Adapter directory)
./mvnw spring-boot:run

# 4. Verify Kafka UI (optional)
# Open browser: http://localhost:18080
```

---

## Monitoring

### Kafka UI

- URL: http://localhost:18080
- View topics, message counts, and consumer lag

### Application Logs

- DatahubPOC: Message ingestion and routing decisions
- Adapter: TCP connections and Kafka consumption
- Consumer: Message receipt and latency calculations

---

## Performance Metrics

### Expected Throughput

| Metric | Typical Value |
|--------|---------------|
| Producer throughput | 400-800 messages/second |
| End-to-end latency | 20-50 milliseconds |
| Kafka processing | Near real-time |
| TCP delivery | Sub-millisecond |

### Latency Tracking

Consumer automatically calculates end-to-end latency:
- Message body format: `index|timestamp`
- Consumer extracts timestamp and calculates elapsed time
- Logs per-message latency and final statistics

---

## Troubleshooting

### No Messages Received

1. Check Adapter is running on port 9293
2. Verify consumer connected (look for "SIGN-ON SUCCESSFUL")
3. Check Kafka topics exist in Kafka UI
4. Verify routing criteria match message characteristics

### Messages Going to Wrong Partner

1. Review routing criteria in routingCriteria.json
2. Check message source (3=Hesse, 6=Berlin)
3. Verify message type and destination
4. Check StreamProcessor logs for routing decisions

### Low Throughput

1. Check system resources (CPU, memory)
2. Verify Kafka broker health
3. Check network connectivity
4. Review application logs for errors

---

## Additional Scripts

### Original Producer

```bash
python3 producer.py <count> <messageType> <destination>
```

**Arguments:**
- count: Number of messages
- messageType: A, B, C, or "-" for random
- destination: 1, 2, 3, 4, or "-" for none

**Note:** Randomly selects source from {3, 6}

**Examples:**
```bash
python3 producer.py 10 A 2
python3 producer.py 100 - -
python3 producer.py 50 B 2
```
