import argparse
import os
import pandas as pd
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

def parse_gtfs_realtime(pb_file_path, output_format='csv', output_file=None):
    """
    Parse a GTFS Realtime protocol buffer file and output the data in CSV or JSON format.
    
    Args:
        pb_file_path (str): Path to the GTFS Realtime .pb file
        output_format (str): Output format ('csv' or 'json')
        output_file (str): Path to the output file (optional)
    
    Returns:
        DataFrame: Pandas DataFrame containing the parsed data
    """
    if not os.path.exists(pb_file_path):
        raise FileNotFoundError(f"File not found: {pb_file_path}")
    
    # Parse the GTFS-realtime protobuf file
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(pb_file_path, "rb") as f:
        feed.ParseFromString(f.read())
    
    # Print basic feed info
    print(f"GTFS Realtime Feed Version: {feed.header.version}")
    if feed.header.HasField("timestamp"):
        from datetime import datetime
        print(f"Feed Timestamp: {datetime.fromtimestamp(feed.header.timestamp)}")
    
    print(f"Number of entities: {len(feed.entity)}")
    
    # Prepare a list to store each entity's data
    data = []
    for entity in feed.entity:
        entity_id = entity.id
        
        # Check what type of entity we have
        if entity.HasField("trip_update"):
            # This is a trip update entity
            trip_update_data = process_trip_update(entity)
            if trip_update_data:
                data.append(trip_update_data)
        
        elif entity.HasField("vehicle"):
            # This is a vehicle position entity
            vehicle_data = process_vehicle_position(entity)
            if vehicle_data:
                data.append(vehicle_data)
        
        elif entity.HasField("alert"):
            # This is an alert entity - using your specific processing
            alert_data = process_alert(entity)
            if alert_data:
                data.append(alert_data)
    
    # Create a pandas DataFrame
    if data:
        df = pd.DataFrame(data)
        
        # Sort by Entity ID
        if "Entity ID" in df.columns:
            df.sort_values("Entity ID", inplace=True)
        
        # Export the data if requested
        if output_file:
            if output_format.lower() == 'csv':
                df.to_csv(output_file, index=False, encoding="utf-8-sig")
                print(f"CSV file created at: {output_file}")
            elif output_format.lower() == 'json':
                df.to_json(output_file, orient="records", force_ascii=False, indent=2)
                print(f"JSON file created at: {output_file}")
        
        return df
    else:
        print("No entities were processed.")
        return pd.DataFrame()

def process_trip_update(entity):
    """Process a trip update entity."""
    trip_update = entity.trip_update
    
    # Basic trip info
    trip_id = trip_update.trip.trip_id if trip_update.trip.HasField("trip_id") else ""
    route_id = trip_update.trip.route_id if trip_update.trip.HasField("route_id") else ""
    
    # Get schedule relationship
    schedule_relationship = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
        trip_update.trip.schedule_relationship) if trip_update.trip.HasField("schedule_relationship") else "SCHEDULED"
    
    # Process stop time updates
    stop_updates = []
    for stop_update in trip_update.stop_time_update:
        stop_id = stop_update.stop_id if stop_update.HasField("stop_id") else ""
        arrival_time = stop_update.arrival.time if stop_update.HasField("arrival") and stop_update.arrival.HasField("time") else ""
        departure_time = stop_update.departure.time if stop_update.HasField("departure") and stop_update.departure.HasField("time") else ""
        
        stop_updates.append(f"{stop_id}:{arrival_time}-{departure_time}")
    
    return {
        "Entity ID": entity.id,
        "Type": "trip_update",
        "Trip ID": trip_id,
        "Route ID": route_id,
        "Schedule Relationship": schedule_relationship,
        "Stop Updates": "; ".join(stop_updates),
        "Timestamp": trip_update.timestamp if trip_update.HasField("timestamp") else ""
    }

def process_vehicle_position(entity):
    """Process a vehicle position entity."""
    vehicle = entity.vehicle
    
    # Basic trip and vehicle info
    trip_id = vehicle.trip.trip_id if vehicle.HasField("trip") and vehicle.trip.HasField("trip_id") else ""
    route_id = vehicle.trip.route_id if vehicle.HasField("trip") and vehicle.trip.HasField("route_id") else ""
    vehicle_id = vehicle.vehicle.id if vehicle.HasField("vehicle") and vehicle.vehicle.HasField("id") else ""
    vehicle_label = vehicle.vehicle.label if vehicle.HasField("vehicle") and vehicle.vehicle.HasField("label") else ""
    
    # Position info
    latitude = vehicle.position.latitude if vehicle.HasField("position") else ""
    longitude = vehicle.position.longitude if vehicle.HasField("position") else ""
    bearing = vehicle.position.bearing if vehicle.HasField("position") and vehicle.position.HasField("bearing") else ""
    speed = vehicle.position.speed if vehicle.HasField("position") and vehicle.position.HasField("speed") else ""
    
    # Current stop info
    current_stop = vehicle.stop_id if vehicle.HasField("stop_id") else ""
    current_status = gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(
        vehicle.current_status) if vehicle.HasField("current_status") else ""
    
    return {
        "Entity ID": entity.id,
        "Type": "vehicle_position",
        "Trip ID": trip_id,
        "Route ID": route_id,
        "Vehicle ID": vehicle_id,
        "Vehicle Label": vehicle_label,
        "Latitude": latitude,
        "Longitude": longitude,
        "Bearing": bearing,
        "Speed": speed,
        "Current Stop": current_stop,
        "Current Status": current_status,
        "Timestamp": vehicle.timestamp if vehicle.HasField("timestamp") else ""
    }

def process_alert(entity):
    """Process an alert entity - following the specific logic from your code."""
    alert = entity.alert
    
    # Combine active periods into a single string ("start-end")
    active_periods = "; ".join(f"{ap.start}-{ap.end}" for ap in alert.active_period) if alert.active_period else ""
    
    # Combine informed entities (listing available fields for each)
    informed = []
    for ie in alert.informed_entity:
        parts = []
        if ie.HasField("route_id"):
            parts.append(f"route_id={ie.route_id}")
        if ie.HasField("stop_id"):
            parts.append(f"stop_id={ie.stop_id}")
        if ie.HasField("agency_id"):
            parts.append(f"agency_id={ie.agency_id}")
        informed.append(" | ".join(parts))
    informed_entities = "; ".join(informed)
    
    # Get cause and effect names (from enum values)
    cause = gtfs_realtime_pb2.Alert.Cause.Name(alert.cause)
    effect = gtfs_realtime_pb2.Alert.Effect.Name(alert.effect)
    
    # Select header_text in language "he" (if available)
    header_text = ""
    for trans in alert.header_text.translation:
        if trans.language == "he" and trans.text:
            header_text = trans.text
            break
    # If no Hebrew text found, use the first available translation
    if not header_text and alert.header_text.translation:
        header_text = alert.header_text.translation[0].text
    
    # Select description_text in language "he" (if available)
    description_text = ""
    for trans in alert.description_text.translation:
        if trans.language == "he" and trans.text:
            description_text = trans.text
            break
    # If no Hebrew text found, use the first available translation
    if not description_text and alert.description_text.translation:
        description_text = alert.description_text.translation[0].text
    
    # Return the collected data as a dictionary
    return {
        "Entity ID": entity.id,
        "Type": "alert",
        "Active Periods": active_periods,
        "Informed Entities": informed_entities,
        "Cause": cause,
        "Effect": effect,
        "Header Text": header_text,
        "Description Text": description_text
    }

def main():
    parser = argparse.ArgumentParser(description='Parse GTFS Realtime Protocol Buffer files')
    parser.add_argument('pb_file', help='Path to the GTFS Realtime .pb file')
    parser.add_argument('--format', choices=['csv', 'json'], default='csv', help='Output format (default: csv)')
    parser.add_argument('--output', help='Output file path (default: based on input filename)')
    
    args = parser.parse_args()
    
    if not args.output:
        # Generate default output filename based on input
        base_name = os.path.splitext(args.pb_file)[0]
        args.output = f"{base_name}.{args.format}"
    
    # Parse the file and generate output
    parse_gtfs_realtime(args.pb_file, args.format, args.output)

if __name__ == "__main__":
    main()
