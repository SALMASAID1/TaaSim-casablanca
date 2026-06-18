# 🚕 Flink Job 2: Demand Aggregator — Technical Specification & Agent Prompt

This document provides a highly detailed, professional, and comprehensive prompt designed to feed an AI agent to implement **Flink Job 2 (Demand Aggregator)** in your codebase.

---

## 🎯 Task Objective

Implement the Flink streaming application **Flink Job 2 (Demand Aggregator)** in Java inside the Maven subproject `flink_jobs` under package `com.taasim.flink.job2`.

This job ingests two real-time streams (`processed.gps` and `raw.trips`), correlates them using **30-second tumbling event-time windows** grouped by city zone, calculates the supply-demand ratio, and sinks the aggregates to Cassandra (`taasim.demand_zones`) and a new Kafka topic (`processed.demand`).

---

## 🏗 Directory & Code Layout

Ensure all new classes are created within these specific packages in `flink_jobs/`:
* **Main Class:** `flink_jobs/src/main/java/com/taasim/flink/job2/Job2DemandAggregator.java`
* **Models:** `flink_jobs/src/main/java/com/taasim/flink/job2/model/`
  * `GpsProcessedEvent.java`
  * `TripRequestEvent.java`
  * `UnifiedWindowInput.java`
  * `DemandZoneAggregate.java`
* **Functions:** `flink_jobs/src/main/java/com/taasim/flink/job2/functions/`
  * Deserializers and process functions.
* **Test Class:** `flink_jobs/src/test/java/com/taasim/flink/job2/Job2DemandAggregatorTest.java`

---

## ⚙️ Data Stream Schemas

### Stream 1: Vehicle GPS (Source Topic: `processed.gps`)
* **Kafka Key:** `taxiId` (String)
* **Kafka Value (JSON):**
  ```json
  {
    "city": "casablanca",
    "zoneId": 15,
    "eventTimeMillis": 1779126923546,
    "taxiId": "20000007",
    "lat": 33.55,
    "lon": -7.5625,
    "speedKmh": 25.0,
    "status": "available"
  }
  ```

### Stream 2: Trip Requests (Source Topic: `raw.trips`)
* **Kafka Key:** `trip_id` (String UUID)
* **Kafka Value (JSON):**
  ```json
  {
    "trip_id": "472b535d-6c1e-450f-a3ff-90a8a6cf71b6",
    "rider_id": "rider-145",
    "origin_zone": 15,
    "destination_zone": 3,
    "requested_at": "2026-05-18T19:54:08.123Z",
    "call_type": "A"
  }
  ```

---

## 🛠 Step-by-Step Implementation Details

### Step 1: Model Classes
Create POJOs/Records under `com.taasim.flink.job2.model`:
1. **`GpsProcessedEvent`**: Maps to `processed.gps` JSON format.
2. **`TripRequestEvent`**: Maps to `raw.trips` JSON format. Add a helper `long getRequestedAtMillis()` to parse `requested_at` (ISO-8601 string) into epoch milliseconds via `java.time.Instant.parse(requestedAt).toEpochMilli()`.
3. **`UnifiedWindowInput`**: A unified structure used to union both streams.
   ```java
   package com.taasim.flink.job2.model;
   
   import java.io.Serializable;
   
   public class UnifiedWindowInput implements Serializable {
       public String city;
       public int zoneId;
       public String eventType; // "VEHICLE" or "REQUEST"
       public String entityId;  // taxiId or tripId
       public long eventTimeMillis;
   }
   ```
4. **`DemandZoneAggregate`**: The output class.
   ```java
   package com.taasim.flink.job2.model;
   
   import java.io.Serializable;
   
   public class DemandZoneAggregate implements Serializable {
       public String city;
       public int zoneId;
       public long windowStart;
       public int activeVehicles;
       public int pendingRequests;
       public float ratio;
       public float forecastDemand = 0.0f; // placeholder for ML forecasting
   }
   ```

### Step 2: Event Time & Watermarking Strategy
Apply watermarks to **both** streams prior to joining or unioning:
* **Lateness Bound:** `Duration.ofMinutes(3)` using Flink's `BoundedOutOfOrderness` watermarks.
* **Timestamp Assigners:**
  * GPS Stream: Extract `eventTimeMillis` as timestamp.
  * Trip Stream: Parse `requested_at` into epoch milliseconds as timestamp.

### Step 3: Stream Union & Keying
1. Map GPS events to `UnifiedWindowInput` (set `eventType = "VEHICLE"`, `entityId = taxiId`, `zoneId = zoneId`).
2. Map Trip events to `UnifiedWindowInput` (set `eventType = "REQUEST"`, `entityId = trip_id`, `zoneId = origin_zone`).
3. **Union** both streams:
   ```java
   DataStream<UnifiedWindowInput> unioned = gpsUnified.union(tripsUnified);
   ```
4. **KeyBy** `zoneId` to parallelize calculations:
   ```java
   KeyedStream<UnifiedWindowInput, Integer> keyed = unioned.keyBy(e -> e.zoneId);
   ```

### Step 4: Window Aggregator Function
1. Apply a Tumbling Event-Time Window of **30 seconds**:
   ```java
   WindowedStream<UnifiedWindowInput, Integer, TimeWindow> windowed = 
       keyed.window(TumblingEventTimeWindows.of(Time.seconds(30)));
   ```
2. Implement a `ProcessWindowFunction` that computes window aggregates:
   * Keep a `Set<String>` of unique active `taxiId`s (from `"VEHICLE"` events).
   * Count all trip requests (from `"REQUEST"` events).
   * **Compute Ratio:**
     $$\text{ratio} = \frac{\text{pendingRequests}}{\max(\text{activeVehicles}, 1)}$$
   * **Emit:** Output a `DemandZoneAggregate` setting `windowStart = context.window().getStart()`.

---

## 💾 Target Sinks

### Cassandra Sink (Table: `taasim.demand_zones`)
* **CQL Insert Query:**
  ```sql
  INSERT INTO taasim.demand_zones (city, zone_id, window_start, active_vehicles, pending_requests, ratio, forecast_demand) 
  VALUES (?, ?, ?, ?, ?, ?, ?);
  ```
* **Mapping:**
  * `city` (String, default "casablanca")
  * `zone_id` (Integer)
  * `window_start` (java.sql.Timestamp, mapped from `windowStart`)
  * `active_vehicles` (Integer)
  * `pending_requests` (Integer)
  * `ratio` (Float)
  * `forecast_demand` (Float, default `0.0f`)
* Use Flink's `CassandraSink.addSink` with mapped Cassandra tuples or class mapping.

### Kafka Sink (Topic: `processed.demand`)
* **Kafka Key:** Stringified `zoneId` (e.g., `"15"`)
* **Kafka Value:** JSON string representing `DemandZoneAggregate` POJO.

---

## ⚙️ CLI Parameter Tool Configurations

Implement dynamic parameters in `Job2DemandAggregator` using Flink's `ParameterTool` exactly as done in Flink Job 1:
* `--kafka-bootstrap-servers` (Default: `kafka:29092`)
* `--gps-source-topic` (Default: `processed.gps`)
* `--trips-source-topic` (Default: `raw.trips`)
* `--demand-sink-topic` (Default: `processed.demand`)
* `--cassandra-host` (Default: `cassandra`)
* `--cassandra-port` (Default: `9042`)
* `--checkpoint-dir` (Default: `s3a://taasim/raw/kafka-archive/flink-checkpoints/job2/`)
* `--checkpoint-interval-ms` (Default: `60000`)

---

## 🧪 Concrete Verification Test

Ensure your pipeline compiles and constructs without executing. Implement `Job2DemandAggregatorTest.java` in `src/test/java/com/taasim/flink/job2/` with the following JUnit 5 code:

```java
package com.taasim.flink.job2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class Job2DemandAggregatorTest {

    @Test
    void buildJob_buildsPipelineWithoutExecuting() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        final ParameterTool params =
                ParameterTool.fromArgs(
                        new String[] {
                            "--kafka-bootstrap-servers", "localhost:9092",
                            "--gps-source-topic", "processed.gps",
                            "--trips-source-topic", "raw.trips",
                            "--demand-sink-topic", "processed.demand",
                            "--cassandra-host", "localhost",
                            "--cassandra-port", "9042",
                            "--checkpoint-dir", "file:///tmp/taasim/flink-checkpoints/job2/",
                            "--checkpoint-interval-ms", "1000"
                        });

        assertDoesNotThrow(() -> Job2DemandAggregator.buildJob(env, params));
    }
}
```

---
*Verify the final implementation by running Maven builds:*
```bash
mvn clean package -pl flink_jobs
```
*And verify all unit tests pass successfully:*
```bash
mvn test -pl flink_jobs -Dtest=Job2DemandAggregatorTest
```
