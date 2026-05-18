# 🚕 Flink Job 3: Trip Matcher — Technical Specification & Agent Prompt

This document provides a highly detailed, professional, and comprehensive prompt designed to feed an AI agent to implement **Flink Job 3 (Trip Matcher)** in your codebase.

---

## 🎯 Task Objective

Implement the Flink streaming application **Flink Job 3 (Trip Matcher)** in Java inside the Maven subproject `flink_jobs` under package `com.taasim.flink.job3`.

This job ingests the passenger requests (`raw.trips`) and real-time vehicle coordinates (`processed.gps`), tracks vehicle availability across all 16 Casablanca zones, and implements a stateful matching algorithm with a **5-second SLA timer** and **adjacent-zone search fallback** (using the zone centroid boundaries in `zone_mapping.csv`). Successful matches are written to Cassandra (`taasim.trips`) and unmatched requests are published to the `raw.unmatched` Kafka topic.

---

## 🏗 Directory & Code Layout

Create all new classes within these packages in the `flink_jobs/` directory:
* **Main Class:** `flink_jobs/src/main/java/com/taasim/flink/job3/Job3TripMatcher.java`
* **Models:** `flink_jobs/src/main/java/com/taasim/flink/job3/model/`
  * `GpsProcessedEvent.java`
  * `TripRequestEvent.java`
  * `VehicleInfo.java`
  * `TripMatchEvent.java`
  * `ZoneDefinition.java` (or reuse from `com.taasim.flink.job1.model.ZoneDefinition`)
* **Functions:** `flink_jobs/src/main/java/com/taasim/flink/job3/functions/`
  * Deserializers and co-process/broadcast functions.
* **Test Class:** `flink_jobs/src/test/java/com/taasim/flink/job3/Job3TripMatcherTest.java`

---

## ⚙️ Data Stream Schemas

### Stream 1: Normalized GPS Positions (Source Topic: `processed.gps`)
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

## 🏗 Flink State & Processing Architecture

To perform the **adjacent-zone search fallback**, an operator must be able to query the state of *other* zones. In a standard Flink Keyed Stream, the state for zone $A$ cannot access the state for zone $B$. 

To solve this limitation elegantly with high performance, we use **Keyed Broadcast State**:
1. Ingest `processed.gps` as a **Broadcast Stream** (since there are only ~5,000 active taxis, storing all vehicle positions in a shared map uses less than 1 MB of memory).
2. Ingest `raw.trips` as a regular stream, **keyed by `origin_zone`**.
3. Co-process both streams using Flink's `KeyedBroadcastProcessFunction`.

### Keyed Broadcast State Design

#### 1. Broadcast State Descriptor:
* Name: `"vehicles"`
* Key: `String` (`taxiId`)
* Value: `VehicleInfo` (POJO containing: `taxiId`, `zoneId`, `lat`, `lon`, `status` ("available" or "assigned"), and `eventTimeMillis` (last seen)).

#### 2. Keyed State Descriptor (Keyed by `origin_zone`):
* Name: `"pending-requests"`
* Key: `String` (`tripId`)
* Value: `TripRequestEvent`

---

## 🛠 Step-by-Step Implementation Details

### Step 1: Ingestion & Event Time
1. Consume `processed.gps` and `raw.trips` using `KafkaSource<String>` with JSON deserializers.
2. Apply `WatermarkStrategy` on **both** streams:
   * GPS Stream: `BoundedOutOfOrderness(3 minutes)` based on `eventTimeMillis`.
   * Trip Stream: `BoundedOutOfOrderness(3 minutes)` based on parsing `requested_at` into epoch milliseconds.

### Step 2: Keyed Broadcast Processing
1. Broadcast the GPS stream:
   ```java
   MapStateDescriptor<String, VehicleInfo> vehicleStateDesc = new MapStateDescriptor<>(
       "vehicles", Types.STRING, TypeInformation.of(VehicleInfo.class)
   );
   BroadcastStream<GpsProcessedEvent> broadcastGps = gpsStream.broadcast(vehicleStateDesc);
   ```
2. Key the Trip stream by `origin_zone` and connect it with the broadcast stream:
   ```java
   DataStream<TripMatchEvent> matches = tripsStream
       .keyBy(TripRequestEvent::getOriginZone)
       .connect(broadcastGps)
       .process(new TripMatcherFunction(vehicleStateDesc));
   ```

### Step 3: Core Matching Logic (`TripMatcherFunction`)

#### Inside `processBroadcastElement` (Receiving GPS Events):
Update the broadcast state:
* Extract the vehicle's `taxiId`.
* If `status` is `"offline"`, remove the vehicle from the broadcast state.
* Otherwise, upsert the `VehicleInfo` (containing `taxiId`, `zoneId`, `lat`, `lon`, `status`, and `eventTimeMillis` as `last_seen`).

#### Inside `processElement` (Receiving Trip Requests):
1. Save the request to the keyed `MapState<String, TripRequestEvent> pendingRequests` state.
2. **First Search (Exact Zone):**
   * Query the broadcast state to find vehicles where `zoneId == request.origin_zone` and `status == "available"`.
   * If multiple available vehicles are found, select the one with the **oldest `last_seen` timestamp**.
3. **If a Vehicle is Found:**
   * Mark the vehicle's status as `"assigned"` in the broadcast state to prevent double-matching.
   * Calculate **ETA** (details below).
   * Emit a successful `TripMatchEvent` and remove the request from the pending state.
4. **If No Vehicle is Found:**
   * Register an event-time or processing-time **SLA timer for 5 seconds** (event-time timestamp: `request.getRequestedAtMillis() + 5000`).

#### Inside `onTimer` (Triggered after 5 seconds):
1. If the trip request is still in the pending state (unmatched):
2. **Second Search (Adjacent Zone Fallback):**
   * Load the adjacency lists from `ZoneMappingLoader.loadZonesFromClasspath()`.
   * Sort adjacent zones in order of ascending distance between centroids (calculated via the Haversine formula).
   * Iterate through the sorted adjacent zones:
     * Query the broadcast state for an available vehicle in the adjacent zone (`status == "available"`).
     * If a vehicle is found:
       * Set its status to `"assigned"` in the broadcast state.
       * Emit a successful `TripMatchEvent` with the flag `matched_zone_differs = true`.
       * Remove the request from pending state and return.
3. **If Still Unmatched:**
   * Emit an `unmatched` event to a side-output or separate stream (Kafka topic `raw.unmatched`).
   * Remove the request from the pending state.

---

## 🧮 Mathematical Calculations

### Centroid Distance & ETA Formula
1. **Haversine Distance ($d$):**
   Given $(lat_1, lon_1)$ and $(lat_2, lon_2)$:
   $$\Delta lat = lat_2 - lat_1$$
   $$\Delta lon = lon_2 - lon_1$$
   $$a = \sin^2\left(\frac{\Delta lat}{2}\right) + \cos(lat_1) \cdot \cos(lat_2) \cdot \sin^2\left(\frac{\Delta lon}{2}\right)$$
   $$d = 2 \cdot R \cdot \arcsin(\sqrt{a}) \quad \text{where } R = 6371 \text{ km}$$
2. **ETA Calculation:**
   $$\text{eta\_seconds} = \max\left(10, \left(\frac{d}{25.0 \text{ km/h}}\right) \cdot 3600\right)$$
   *(We enforce a minimum ETA of 10 seconds to account for driver startup).*

---

## 💾 Target Sinks

### Cassandra Sink (Table: `taasim.trips`)
* **CQL Insert Query:**
  ```sql
  INSERT INTO taasim.trips (city, date_bucket, created_at, trip_id, rider_id, taxi_id, origin_zone, dest_zone, status, fare, eta_seconds, matched_within_sla) 
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
  ```
* **Type Mapping:**
  * `city` (String, default "casablanca")
  * `date_bucket` (java.time.LocalDate, derived from `requested_at` timestamp)
  * `created_at` (java.sql.Timestamp, mapped from `requested_at` timestamp)
  * `trip_id` (java.util.UUID)
  * `rider_id` (String)
  * `taxi_id` (String)
  * `origin_zone` (Integer)
  * `dest_zone` (Integer)
  * `status` (String, e.g. `"matched"`)
  * `fare` (java.math.BigDecimal, calculated at a flat base rate, e.g., 10.00 MAD)
  * `eta_seconds` (Integer)
  * `matched_within_sla` (Boolean: `true` if matched in exact zone, `false` if matched in adjacent zone via fallback).

### Kafka Sink (Topic: `raw.unmatched`)
* **Kafka Value:** JSON string representing the unmatched passenger request.

---

## ⚙️ CLI Parameter Tool Configurations

Implement dynamic parameters in `Job3TripMatcher` using Flink's `ParameterTool` exactly as done in previous jobs:
* `--kafka-bootstrap-servers` (Default: `kafka:29092`)
* `--gps-source-topic` (Default: `processed.gps`)
* `--trips-source-topic` (Default: `raw.trips`)
* `--unmatched-sink-topic` (Default: `raw.unmatched`)
* `--cassandra-host` (Default: `cassandra`)
* `--cassandra-port` (Default: `9042`)
* `--checkpoint-dir` (Default: `s3a://taasim/raw/kafka-archive/flink-checkpoints/job3/`)
* `--checkpoint-interval-ms` (Default: `60000`)

---

## 🧪 Concrete Verification Test

Ensure your pipeline compiles and constructs without executing. Implement `Job3TripMatcherTest.java` in `src/test/java/com/taasim/flink/job3/` with the following JUnit 5 code:

```java
package com.taasim.flink.job3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class Job3TripMatcherTest {

    @Test
    void buildJob_buildsPipelineWithoutExecuting() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        final ParameterTool params =
                ParameterTool.fromArgs(
                        new String[] {
                            "--kafka-bootstrap-servers", "localhost:9092",
                            "--gps-source-topic", "processed.gps",
                            "--trips-source-topic", "raw.trips",
                            "--unmatched-sink-topic", "raw.unmatched",
                            "--cassandra-host", "localhost",
                            "--cassandra-port", "9042",
                            "--checkpoint-dir", "file:///tmp/taasim/flink-checkpoints/job3/",
                            "--checkpoint-interval-ms", "1000"
                        });

        assertDoesNotThrow(() -> Job3TripMatcher.buildJob(env, params));
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
mvn test -pl flink_jobs -Dtest=Job3TripMatcherTest
```
