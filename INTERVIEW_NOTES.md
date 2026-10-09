# Interview Notes

## Project Problem

Urban traffic conditions change continuously. A traditional batch pipeline
may detect congestion only after it has already occurred.

The project processes continuously generated vehicle and road-incident
events to detect traffic conditions in near real time while retaining
historical information for analytics.

---

## Day 1

### Why does this project require streaming?

Traffic conditions can change within minutes. Processing events continuously
allows congestion indicators to be calculated while the condition is
occurring instead of waiting for a scheduled batch process.

### Why not simply use CSV files?

CSV files are suitable for many batch workloads, but manually uploading files
does not represent a continuous event source. The project therefore uses a
traffic simulator that continuously generates events.

### Is the traffic data real?

The dynamic vehicle and incident events are simulated. The streaming
architecture itself is real. The ingestion layer is designed so that a real
GPS, IoT, API, or other event source could later replace the simulator.

### Why separate the source from processing?

Decoupling the source from downstream processing allows different producers
to publish a common event format without requiring the processing pipeline
to be redesigned for every source.

## Day 2 — Traffic Event Simulator

### What was built
Python-based continuous traffic simulator that generates vehicle movement and road-incident events using synthetic road reference data.

### Core flow
roads.json → road loader → shared traffic state → vehicle/incident generators → continuous simulator

### Components
- `roads.json` — synthetic road reference/master data.
- `VehicleEvent` — contract for vehicle movement events.
- `IncidentEvent` — contract for road incident events.
- `road_loader.py` — loads road reference data from JSON.
- `traffic_state.py` — maintains current road traffic conditions and active incidents.
- `vehicle_generator.py` — generates vehicle events using road conditions.
- `incident_generator.py` — generates incidents and modifies affected road state.
- `run_traffic_simulator.py` — continuously orchestrates vehicle generation, incidents, traffic updates and recovery.

### Important concepts
- Reference data is relatively static; vehicle/incident events are dynamic.
- Event IDs use UUID-based identifiers.
- Event timestamps are stored in UTC.
- Rush hour changes the probability of light/normal/heavy traffic.
- Weighted probability does not guarantee an outcome; it changes its likelihood.
- Incidents modify shared road state, which indirectly changes vehicle speeds.
- Active incidents prevent normal traffic updates from overwriting incident-affected roads.
- Incident recovery removes finished incidents and restores road state.
- Shared state allows vehicle and incident generators to communicate without directly calling each other.
- Simulator-generated traffic is synthetic; the streaming architecture will be real.

### Key Python concepts used
- dataclasses
- lists and dictionaries
- dictionary/list comprehensions
- functions
- imports
- random.choice / random.choices / random.uniform
- UUID generation
- UTC datetime
- loops and conditions
- try/except KeyboardInterrupt
- shared mutable state

### Interview explanation
I created a Python traffic simulator because I did not have access to a production GPS or traffic-sensor feed. It continuously generates structured vehicle and incident events using synthetic road reference data. Traffic conditions vary using weighted probabilities, and incidents modify road-level state so affected vehicles experience lower speeds. The simulator replaces only the physical source; downstream ingestion and processing will be designed so a real GPS, IoT or API source can later replace it.


## Day 3 — Docker + Apache Kafka Fundamentals
Purpose: Kafka sits between event producers and consumers, decoupling them and allowing events to be retained and consumed independently.
Flow: Producer → Kafka Broker → Topic → Partition → Consumer
Docker: Image = packaged software/template, Container = running instance of image, Docker Compose = YAML-based definition for running/configuring services, Port mapping `9092:9092` = host 9092 → container 9092.
Kafka setup: Apache Kafka 4.1.0, single local Kafka node using KRaft, Node ID=1, roles=broker+controller, client listener=9092, controller listener=9093, advertised client address=localhost:9092, local protocol=PLAINTEXT.
Broker = Kafka server that receives/stores/serves records, Controller = manages Kafka cluster metadata/coordination, Listener = network endpoint Kafka opens for connections, Advertised Listener = address Kafka tells clients to use.
Topics: `traffic.vehicle.events`, `traffic.incident.events`, each configured with 3 partitions and replication factor 1.
Topic = named event stream/category, Partition = lane inside a topic enabling distribution/parallelism, Offset = record position within one partition; offsets are partition-specific.
Ordering: Kafka guarantees ordering within a partition, not global ordering across all topic partitions.
Key: Producer can attach a key such as `road_id`; normal key-based partitioning keeps the same key mapped consistently to the same partition while partition configuration remains unchanged, helping preserve per-key ordering.
Leader = broker responsible for partition reads/writes, Replica = broker holding a partition copy, ISR = replicas currently in sync with leader, Replication Factor=1 because local setup has one broker and therefore no broker-level fault tolerance.
Producer = writes records to Kafka, Consumer = reads records from Kafka, Consumer Group = consumers working together; within one group a partition is assigned to at most one consumer at a time, so 3 partitions allow at most 3 actively assigned consumers for that topic at once.
Consumer offset = group's processing position, Log End Offset = end of available partition log, Lag = records the consumer group has not caught up with; large lag can indicate downstream processing cannot keep up.
`--from-beginning` = read retained existing records as well as continuing to consume new ones.
Verified: Docker Kafka container running, both project topics created, manual JSON produced to `traffic.vehicle.events`, Kafka retained the record, consumer successfully retrieved it, partition/offset inspected.
Day 3 architecture: Manual Producer → Kafka (`traffic.vehicle.events` / `traffic.incident.events`) → Manual Consumer.
Next: Day 4 replaces manual producer with Python traffic simulator → Kafka.

## Day 4 — Python Simulator to Kafka Integration
Purpose: Connected the Python traffic simulator to Apache Kafka so dynamically generated vehicle and incident events are continuously published to separate Kafka topics.
Flow: Python Traffic Simulator → KafkaEventProducer → JSON Serialization → Kafka Broker → Topics → Consumers
Python Kafka Client: Used `confluent-kafka` to create a reusable Kafka producer.
Bootstrap Server: `localhost:9092` is the initial Kafka broker address used by the Python producer to connect to Kafka.
Serialization: Python event object → dictionary → JSON string → Kafka record. Serialization converts application data into a transferable format; consumers later deserialize it back into usable data.
Topics: Vehicle events → `traffic.vehicle.events`, Incident events → `traffic.incident.events`.
Kafka Record: Topic = destination stream, Key = `road_id`, Value = serialized JSON event.
Key-based Partitioning: `road_id` is used as the Kafka key. Events with the same road key are consistently routed to the same partition while partition configuration remains unchanged, helping preserve per-road ordering.
Observed Example: `ROAD_001` incident events repeatedly arrived in Partition 1, demonstrating consistent key-based partitioning.
Asynchronous Producer: Kafka `produce()` queues records for asynchronous delivery rather than waiting for every message to complete before continuing.
`poll(0)`: Allows the producer to process Kafka delivery callbacks/events without blocking the simulator.
Delivery Callback: Added callback-based error reporting so failed Kafka deliveries can be detected without printing success logs for every event.
`flush()`: Waits for queued Kafka messages to finish delivery before the application exits.
Graceful Shutdown: `KeyboardInterrupt` handles Ctrl+C and `finally` guarantees producer cleanup/flush during normal interruption.
Simulation Rate: One simulation cycle runs approximately every 3 seconds using `time.sleep(3)`.
Verified: Vehicle events continuously reached `traffic.vehicle.events`, incident events independently reached `traffic.incident.events`, Kafka keys/partitions/offsets were visible through console consumers, and graceful producer shutdown was verified.
Debugging Lesson: Printing an event in the simulator does not prove Kafka received it. Verify each pipeline boundary independently using the Kafka consumer.
Python Lesson: Incorrect indentation placed `time.sleep()` outside `while True`, causing events to be generated continuously. Moving it inside the loop correctly controlled the simulation rate.
Day 4 Architecture: Python Simulator → Kafka Producer → Vehicle/Incident Topics → Kafka Consumers.
Next: Spark Structured Streaming will eventually replace the console consumer and process Kafka events in real time.

## Day 5 — Kafka Consumer and Data Validation
Purpose: Built a Python Kafka consumer to read, deserialize, validate and process vehicle events from Kafka.
Flow: Traffic Simulator → Kafka Producer → Kafka Topic → Python Consumer → JSON Deserialization → Data Validation → Valid/Rejected Output.
Kafka Consumer: Used `confluent-kafka.Consumer` to read records from `traffic.vehicle.events`.
Consumer Group: `group.id` identifies consumers that cooperate to process topic partitions. Our group is `traffic-vehicle-validator`.
Subscribe: `consumer.subscribe(topics)` registers the topics the consumer reads.
Poll: `consumer.poll(1.0)` waits up to one second for an available message.
Deserialization: Converts Kafka message bytes → UTF-8 string → Python dictionary using `json.loads()`.
Validation: Checks required fields, numeric speed, speed range (0–200 km/h), latitude (-90 to 90) and longitude (-180 to 180).
Valid Event: Passes all configured validation checks and is printed as VALID.
Rejected Event: Fails validation or JSON parsing and is printed as REJECTED.
Consumer Offset: Position used to track progress through a Kafka partition. Offsets are partition-specific.
Manual Commit: `enable.auto.commit=False` disables automatic commits. `consumer.commit(asynchronous=False)` synchronously commits processed offsets.
At-Least-Once Processing: Committing after successful handling helps avoid losing unprocessed records, but failures before commit may cause records to be processed again.
Auto Offset Reset: `earliest` starts from the earliest retained record only when the consumer group has no committed offset.
Consumer Lag: Difference between the latest available Kafka offset and the consumer group's progress. A slow consumer can accumulate lag.
Graceful Shutdown: `consumer.close()` releases resources and leaves the consumer group cleanly.
Testing: Tested valid event, invalid speed, missing road_id and invalid latitude; all four validation tests passed.
Current Limitation: Rejected records are printed and committed, not yet stored in a dead-letter topic or quarantine store.
Next: Extend streaming processing and introduce more advanced data-quality checks.


Why are we adding spark.jars.packages?
- pyspark gives us Spark's Python API.
- spark-sql-kafka-0-10_2.12 lets Spark Structured Streaming communicate with Kafka.
- 3.5.6 matches our installed Spark version.

## Day 6 — Apache Spark Structured Streaming
Purpose: Integrated Apache Spark Structured Streaming with Kafka to consume live vehicle traffic events.
Flow: Python Traffic Simulator → Kafka Producer → Kafka Topic → Spark Structured Streaming → Console.
PySpark: Python API for Apache Spark, used for distributed data processing.
SparkSession: Entry point for creating Spark DataFrames and running Spark applications.
Local Mode: `local[2]` runs Spark locally using two worker threads.
Kafka Connector: `spark-sql-kafka-0-10_2.12:3.5.6` enables Spark 3.5.6 to communicate with Kafka.
readStream: Creates a streaming DataFrame that continuously processes incoming data.
Kafka Source: `format("kafka")` reads records from Apache Kafka.
Bootstrap Servers: `localhost:9092` identifies the Kafka broker endpoint.
Subscribe: Reads events from the `traffic.vehicle.events` topic.
Starting Offsets: `latest` starts a new query from the latest Kafka offsets when no checkpoint exists.
Kafka Message Format: Kafka keys and values arrive as binary data; we cast them to strings.
Kafka Metadata: Spark exposes topic, partition, offset and timestamp.
Micro-Batch Processing: Spark processes available streaming records in small batches.
Processing Trigger: `processingTime="5 seconds"` requests a new micro-batch approximately every five seconds.
Console Sink: `writeStream.format("console")` displays streaming records for development and debugging.
Append Mode: Outputs newly processed rows rather than rewriting previous output.
Streaming Query: `.start()` launches processing; `query.isActive` indicates whether the query is running.
Graceful Shutdown: `query.stop()` stops the stream and `spark.stop()` releases Spark resources.
Checkpointing: Spark supports checkpoint-based recovery, but we have not configured a persistent checkpoint yet.
Testing: Verified live Kafka events in Spark micro-batches and successful Ctrl+C shutdown.
Current Limitation: Kafka JSON is still a string; typed schema parsing, validation, watermarking and congestion calculations are pending.
Next: Parse vehicle JSON into structured columns and begin real-time traffic transformations.

## Day 7 — Spark JSON Parsing and Data Quality
Purpose: Convert Kafka vehicle JSON into structured Spark DataFrames and validate traffic events.
Flow: Simulator → Kafka → Spark Structured Streaming → JSON Parsing → Schema Enforcement → Validation → Console.
StructType: Defines the expected structure of incoming JSON records.
StructField: Defines individual column names, types and nullability.
from_json(): Parses a JSON string into a Spark struct using a predefined schema.
DoubleType: Stores floating-point values such as speed, latitude and longitude.
withColumn(): Creates or replaces a DataFrame column.
to_timestamp(): Converts event-time strings into Spark timestamps.
filter(): Selects records matching a condition; useful for separating valid and rejected data.
when()/otherwise(): Implements conditional logic similar to SQL CASE WHEN.
Data Quality: Checks required fields, non-empty identifiers, valid timestamps, speed range and GPS coordinate ranges.
Kafka Metadata: Preserves topic, partition, offset and timestamp for debugging.
Raw JSON: Retained to investigate malformed or rejected events.
Testing: Validated a correct vehicle event and rejected an event with speed 250 km/h.
Limitation: Invalid records are flagged but not yet stored in quarantine; malformed JSON handling and stricter schema checks can be improved.
Next: Add streaming aggregations, event-time windows and congestion calculations.

## Day 8 — Real-Time Traffic Aggregation
Purpose: Calculate road-level traffic metrics and detect congestion using Spark Structured Streaming.
Flow: Simulator → Kafka → Spark Parsing → Validation → Event-Time Window → Aggregation → Congestion Classification.
Event Time: Time when an event occurred, rather than when Spark processed it.
Window: Groups records within a specified time interval; implemented one-minute tumbling windows.
Watermark: Configured 30-second event-time watermark to manage late data and aggregation state.
groupBy(): Groups vehicle events by road_id and event-time window.
avg(): Calculates average vehicle speed per road/window.
count(): Counts observed vehicle events per road/window.
Update Mode: Emits updated aggregation results as additional events arrive.
Micro-Batch: Spark processes newly available records in small batches using a five-second trigger.
Congestion Rules: HEAVY below 15 km/h, MODERATE from 15 to below 30 km/h, FREE_FLOW at 30 km/h or above.
Data Quality: Only VALID vehicle events contribute to traffic aggregations.
Testing: Verified local aggregation tests and live Kafka streaming across all five roads.
Result: Observed HEAVY, MODERATE and FREE_FLOW, including changing congestion classifications.
Limitation: Event count is not unique vehicle count; speed thresholds are provisional and not road-specific.
Next: Improve congestion metrics, event-time processing, state management and checkpoint recovery.