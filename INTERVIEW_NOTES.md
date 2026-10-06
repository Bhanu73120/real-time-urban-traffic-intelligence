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