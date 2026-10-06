from simulator.generators.vehicle_generator import generate_vehicle_event


for _ in range(10):
    event = generate_vehicle_event()
    print(event.to_dict())