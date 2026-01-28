# Screenshot Capture Guide - Datahub POC

**Purpose**: Step-by-step guide to capture all screenshots for the benchmark documentation

**Save Location**: `/Users/taoisttemper/Projects/Datahub/docs/screenshots/`

---

## Preparation

### Step 1: Create Screenshots Directory

```bash
mkdir -p /Users/taoisttemper/Projects/Datahub/docs/screenshots
cd /Users/taoisttemper/Projects/Datahub/docs/screenshots
```

### Step 2: Verify All Services Are Running

```bash
# Check DatahubPOC
curl -s http://localhost:8080/actuator/health | jq '.status'

# Check Adapter
curl -s http://localhost:8180/actuator/health | jq '.status'

# Check Prometheus
curl -s http://localhost:9090/-/healthy

# Check Grafana
curl -s http://localhost:3000/api/health | jq '.database'
```

**Expected**: All should return "UP" or "ok"

---

## Part 1: Prometheus Screenshots (7 screenshots)

### Screenshot 1: Routing Operations Count

**File**: `prometheus-routing-count.png`

**Steps**:
1. Open browser: http://localhost:9090
2. In the query box, paste:
   ```
   datahub_routing_time_seconds_count
   ```
3. Click "Execute" button
4. Switch to "Graph" tab (if not already selected)
5. **What to capture**:
   - Query box showing the metric name
   - The result showing value ~40,175
   - The graph visualization
   - Time range selector at top

**Expected Result**:
```
datahub_routing_time_seconds_count{
  class="com.db.datahubpoc.processor.service.MessageProcessingService",
  exception="none",
  instance="host.docker.internal:8080",
  job="datahub-app",
  method="getOutgoingPartnerInterfaces"
} = 40175
```

---

### Screenshot 2: Conversion Operations Count

**File**: `prometheus-conversion-count.png`

**Steps**:
1. Clear previous query
2. In the query box, paste:
   ```
   datahub_conversion_time_seconds_count
   ```
3. Click "Execute"
4. **What to capture**:
   - Full query interface
   - Result showing value ~52,014
   - Graph showing the increase over time
   - Step increases during each load test

**Expected Result**:
```
datahub_conversion_time_seconds_count{...} = 52014
```

---

### Screenshot 3: Average Routing Time

**File**: `prometheus-avg-routing-time.png`

**Steps**:
1. In the query box, paste:
   ```
   (datahub_routing_time_seconds_sum / datahub_routing_time_seconds_count) * 1000
   ```
2. Click "Execute"
3. **What to capture**:
   - Formula in query box
   - Result showing ~0.012 milliseconds
   - Graph visualization
   - Note the ultra-low value

**Expected Result**:
```
{} = 0.012
```

---

### Screenshot 4: Average Conversion Time

**File**: `prometheus-avg-conversion-time.png`

**Steps**:
1. In the query box, paste:
   ```
   (datahub_conversion_time_seconds_sum / datahub_conversion_time_seconds_count) * 1000
   ```
2. Click "Execute"
3. **What to capture**:
   - Calculated average displayed
   - Result showing ~0.108 milliseconds
   - Graph showing stability over time

**Expected Result**:
```
{} = 0.108
```

---

### Screenshot 5: Fan-out Ratio

**File**: `prometheus-fanout-ratio.png`

**Steps**:
1. In the query box, paste:
   ```
   datahub_conversion_time_seconds_count / datahub_routing_time_seconds_count
   ```
2. Click "Execute"
3. **What to capture**:
   - Current ratio value
   - Result showing ~1.29
   - Graph showing how ratio changed

**Expected Result**:
```
{} = 1.29
```

---

### Screenshot 6: Operations Rate (Dual Query)

**File**: `prometheus-operations-rate.png`

**Steps**:
1. Click "Add Query" button (+ icon)
2. In Query A, paste:
   ```
   rate(datahub_routing_time_seconds_count[1m])
   ```
3. In Query B, paste:
   ```
   rate(datahub_conversion_time_seconds_count[1m])
   ```
4. Click "Execute"
5. Switch to "Graph" tab
6. **What to capture**:
   - Both query boxes visible
   - Dual line graph
   - Peak rates during testing
   - Legend showing both metrics
   - Flat lines (0) between tests
   - Spikes during load tests

---

### Screenshot 7: Targets Status

**File**: `prometheus-targets.png`

**Steps**:
1. Click "Status" menu at top
2. Select "Targets"
3. **What to capture**:
   - All targets listed
   - Health status (should be green/UP for all)
   - Last scrape times
   - Scrape intervals
   - Focus on these targets:
     - datahub-app (host.docker.internal:8080)
     - adapter-app (host.docker.internal:8180)
     - kafka (broker:9404)
     - prometheus (localhost:9090)

**Expected**: All targets showing GREEN/UP status

---

## Part 2: Grafana Screenshots (8 screenshots)

### Screenshot 1: Data Source Configuration

**File**: `grafana-datasource.png`

**Steps**:
1. Open browser: http://localhost:3000
2. Login with: admin / admin (if prompted)
3. Click gear icon (⚙️) on left sidebar → "Data sources"
4. If Prometheus data source doesn't exist:
   - Click "Add data source"
   - Select "Prometheus"
   - URL: `http://prometheus:9090`
   - Click "Save & Test"
5. **What to capture**:
   - Data source configuration page
   - URL field showing `http://prometheus:9090`
   - "Data source is working" success message (green)
   - Save & Test button

---

### Screenshot 2: Create New Dashboard

**File**: `grafana-dashboard-create.png`

**Steps**:
1. Click "+" icon on left sidebar
2. Select "Dashboard"
3. Click "Add visualization"
4. Select "Prometheus" as data source
5. **What to capture**:
   - New dashboard interface
   - Panel edit mode
   - Query builder interface

---

### Screenshot 3: Routing Operations Panel

**File**: `grafana-routing-ops-panel.png`

**Steps**:
1. In the query builder:
   - Switch to "Code" mode
   - Paste query: `datahub_routing_time_seconds_count`
2. On the right panel:
   - Panel title: "Routing Operations Count"
   - Visualization type: "Stat"
   - In "Standard options" → Unit: "short" or "ops"
3. Click "Apply"
4. **What to capture**:
   - Panel showing value ~40,175
   - Query visible in edit mode
   - Panel configuration options

---

### Screenshot 4: Conversion Operations Panel

**File**: `grafana-conversion-ops-panel.png`

**Steps**:
1. Click "Add" → "Visualization"
2. Query: `datahub_conversion_time_seconds_count`
3. Panel title: "Conversion Operations Count"
4. Visualization: "Stat"
5. **What to capture**:
   - Panel showing value ~52,014
   - Configuration visible

---

### Screenshot 5: Fan-out Ratio Panel

**File**: `grafana-fanout-panel.png`

**Steps**:
1. Add new visualization
2. Query: `datahub_conversion_time_seconds_count / datahub_routing_time_seconds_count`
3. Panel title: "Fan-out Ratio"
4. Visualization: "Stat"
5. Standard options → Decimals: 2
6. **What to capture**:
   - Panel showing value ~1.29
   - Query formula visible

---

### Screenshot 6: Gauge Panels (Both Latencies)

**File**: `grafana-gauges.png`

**Steps**:
1. Add visualization for Routing Time:
   - Query: `(datahub_routing_time_seconds_sum / datahub_routing_time_seconds_count) * 1000`
   - Panel title: "Average Routing Time"
   - Visualization: "Gauge"
   - Unit: "ms"
   - Thresholds:
     - Base: Green
     - 0.5: Yellow
     - 1: Red
   - Min: 0, Max: 1

2. Add visualization for Conversion Time:
   - Query: `(datahub_conversion_time_seconds_sum / datahub_conversion_time_seconds_count) * 1000`
   - Panel title: "Average Conversion Time"
   - Visualization: "Gauge"
   - Unit: "ms"
   - Same thresholds

3. **What to capture**:
   - Both gauge panels side-by-side
   - Green indicators (good performance)
   - Actual values: ~0.012ms and ~0.108ms
   - Threshold zones visible (green/yellow/red)

---

### Screenshot 7: Time Series Panel

**File**: `grafana-timeseries.png`

**Steps**:
1. Add visualization
2. Panel title: "Operations Rate"
3. Visualization: "Time series"
4. Add two queries:
   - Query A: `rate(datahub_routing_time_seconds_count[1m])` (Legend: "Routing Rate")
   - Query B: `rate(datahub_conversion_time_seconds_count[1m])` (Legend: "Conversion Rate")
5. Unit: "ops"
6. **What to capture**:
   - Time series graph showing both lines
   - Spikes during load tests
   - Legend showing both metrics
   - Tooltip on hover

---

### Screenshot 8: Complete Dashboard Overview

**File**: `grafana-dashboard-overview.png`

**Steps**:
1. After creating all panels, arrange them nicely
2. Click "Save dashboard" (disk icon at top)
3. Name: "Datahub POC - Performance Metrics"
4. Click "Save"
5. Exit edit mode
6. **What to capture**:
   - Full dashboard with all panels visible
   - Dashboard title at top
   - All current values displayed
   - Time range selector (top right)
   - Clean, organized layout

**Expected Layout**:
```
┌────────────────────────────────────────────────┐
│  Datahub POC - Performance Metrics             │
├─────────────┬─────────────┬────────────────────┤
│ Routing Ops │ Conv Ops    │ Fan-out Ratio      │
│  [40,175]   │  [52,014]   │    [1.29]          │
├─────────────┴─────────────┴────────────────────┤
│ Avg Routing Time  │ Avg Conversion Time        │
│  [Gauge: 0.012ms] │ [Gauge: 0.108ms]           │
├───────────────────────────────────────────────-┤
│ Operations Rate Over Time                      │
│ [Time Series Graph]                            │
└────────────────────────────────────────────────┘
```

---

## Part 3: Terminal/System Screenshots (6 screenshots)

### Screenshot 1: Kafka UI - Topics

**File**: `kafka-ui-topics.png`

**Steps**:
1. Open browser: http://localhost:18080
2. Click on "Topics" in left menu
3. **What to capture**:
   - List of all topics
   - Show these topics clearly:
     - IncomingTopic
     - PI1Incoming, PI2Outgoing
     - PI3Incoming, PI4Outgoing, PI5Outgoing
     - PI6Incoming, PI7Outgoing
     - PI8Outgoing
   - Message counts
   - Topic configurations

---

### Screenshot 2: Kafka UI - Messages

**File**: `kafka-ui-messages.png`

**Steps**:
1. In Kafka UI, click on "IncomingTopic"
2. Click "Messages" tab
3. **What to capture**:
   - Sample messages visible
   - Message structure (headers, body)
   - Timestamps
   - Partition information
   - Message content (JSON format)

---

### Screenshot 3: Producer Running

**File**: `producer-running.png`

**Steps**:
1. Open a terminal
2. Run this command:
   ```bash
   cd /Users/taoisttemper/Projects/Datahub/PartnerSimulator
   python3 producer.py 20 A 2
   ```
3. **What to capture**:
   - Terminal showing the producer script running
   - Log messages showing:
     - Application started with args
     - Processing message X of 20
     - Message sent successfully
   - Command prompt visible at top
   - Timestamps in logs

---

### Screenshot 4: Metrics Endpoint - DatahubPOC

**File**: `actuator-datahub.png`

**Steps**:
1. In terminal, run:
   ```bash
   curl -s http://localhost:8080/actuator/prometheus | grep -E "datahub_routing_time|datahub_conversion_time" | head -20
   ```
2. **What to capture**:
   - Terminal output showing metrics
   - Metric names clearly visible
   - Current values
   - Metric metadata (class, method, etc.)
   - Command at top

**Expected Output**:
```
# HELP datahub_routing_time_seconds
# TYPE datahub_routing_time_seconds summary
datahub_routing_time_seconds_count{...} 40175
datahub_routing_time_seconds_sum{...} 0.476147
...
```

---

### Screenshot 5: Health Checks

**File**: `health-checks.png`

**Steps**:
1. In terminal, run:
   ```bash
   echo "=== DatahubPOC Health ===" && \
   curl -s http://localhost:8080/actuator/health | jq && \
   echo "" && \
   echo "=== Adapter Health ===" && \
   curl -s http://localhost:8180/actuator/health | jq
   ```
2. **What to capture**:
   - Both health check outputs
   - Status: "UP"
   - Component statuses
   - Disk space info
   - Formatted JSON output

---

### Screenshot 6: Load Test Summary

**File**: `load-test-summary.png`

**Steps**:
1. In terminal, run:
   ```bash
   curl -s "http://localhost:9090/api/v1/query?query=datahub_routing_time_seconds_count" | \
   jq -r '.data.result[0].value[1] as $routing |
          "Routing Operations: \($routing)"' && \
   curl -s "http://localhost:9090/api/v1/query?query=datahub_conversion_time_seconds_count" | \
   jq -r '.data.result[0].value[1] as $conversion |
          "Conversion Operations: \($conversion)"' && \
   curl -s "http://localhost:9090/api/v1/query?query=datahub_routing_time_seconds_sum" | \
   jq -r '.data.result[0].value[1] as $sum |
          "Total Routing Time: \($sum)s"' && \
   curl -s "http://localhost:9090/api/v1/query?query=datahub_conversion_time_seconds_sum" | \
   jq -r '.data.result[0].value[1] as $sum |
          "Total Conversion Time: \($sum)s"'
   ```
2. **What to capture**:
   - Summary statistics output
   - All four metrics visible
   - Clean, readable format

---

## Screenshot Checklist

Use this to track your progress:

**Prometheus** (7 screenshots):
- [ ] prometheus-routing-count.png
- [ ] prometheus-conversion-count.png
- [ ] prometheus-avg-routing-time.png
- [ ] prometheus-avg-conversion-time.png
- [ ] prometheus-fanout-ratio.png
- [ ] prometheus-operations-rate.png
- [ ] prometheus-targets.png

**Grafana** (8 screenshots):
- [ ] grafana-datasource.png
- [ ] grafana-dashboard-create.png
- [ ] grafana-routing-ops-panel.png
- [ ] grafana-conversion-ops-panel.png
- [ ] grafana-fanout-panel.png
- [ ] grafana-gauges.png
- [ ] grafana-timeseries.png
- [ ] grafana-dashboard-overview.png

**System/Terminal** (6 screenshots):
- [ ] kafka-ui-topics.png
- [ ] kafka-ui-messages.png
- [ ] producer-running.png
- [ ] actuator-datahub.png
- [ ] health-checks.png
- [ ] load-test-summary.png

**Total**: 21 screenshots

---

## Tips for Good Screenshots

1. **Resolution**: Use full screen or large window size
2. **Clarity**: Make sure text is readable
3. **Context**: Include relevant UI elements (menus, buttons)
4. **Clean**: Close unnecessary windows/tabs
5. **Highlight**: If possible, highlight important values
6. **Format**: Save as PNG for best quality

---

## After Capturing Screenshots

1. Verify all 21 screenshots are saved in:
   ```
   /Users/taoisttemper/Projects/Datahub/docs/screenshots/
   ```

2. Check file names match exactly as specified

3. Let me know when complete, and I'll help verify image references in the documentation

---

## Questions or Issues?

If any step is unclear or you encounter issues:
1. Let me know which step you're on
2. Describe what you see vs. what's expected
3. I can provide alternative commands or clarification
