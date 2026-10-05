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