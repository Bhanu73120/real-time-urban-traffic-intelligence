from simulator.generators.incident_generator import generate_incident


for _ in range(5):
    incident = generate_incident()
    print(incident.to_dict())