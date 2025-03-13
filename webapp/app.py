from flask import Flask, render_template, request, send_file, jsonify
import tempfile
import os
import pandas as pd
from google.transit import gtfs_realtime_pb2
import io
import json
from datetime import datetime

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    if not file.filename.endswith('.pb'):
        return jsonify({"error": "File must be a .pb file"}), 400
    
    try:
        # Save the uploaded file to a temporary location
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, file.filename)
        file.save(temp_path)
        
        # Parse the file
        feed = gtfs_realtime_pb2.FeedMessage()
        with open(temp_path, "rb") as f:
            feed.ParseFromString(f.read())
        
        # Prepare data for response
        response_data = {
            "feed_info": {
                "version": feed.header.version,
                "timestamp": datetime.fromtimestamp(feed.header.timestamp).isoformat() if feed.header.HasField("timestamp") else None,
                "entity_count": len(feed.entity)
            },
            "entities": []
        }
        
        # Process entities
        data = []
        for entity in feed.entity:
            entity_id = entity.id
            
            if entity.HasField("alert"):
                alert_data = process_alert(entity)
                if alert_data:
                    data.append(alert_data)
                    response_data["entities"].append({
                        "id": entity_id,
                        "type": "alert",
                        "data": alert_data
                    })
            elif entity.HasField("trip_update"):
                trip_update_data = process_trip_update(entity)
                if trip_update_data:
                    data.append(trip_update_data)
                    response_data["entities"].append({
                        "id": entity_id,
                        "type": "trip_update",
                        "data": trip_update_data
                    })
            elif entity.HasField("vehicle"):
                vehicle_data = process_vehicle_position(entity)
                if vehicle_data:
                    data.append(vehicle_data)
                    response_data["entities"].append({
                        "id": entity_id,
                        "type": "vehicle",
                        "data": vehicle_data
                    })
        
        # Create a DataFrame
        df = pd.DataFrame(data)
        if "Entity ID" in df.columns:
            df.sort_values("Entity ID", inplace=True)
        
        # Save DataFrame to temporary CSV file
        csv_path = os.path.join(temp_dir, os.path.splitext(file.filename)[0] + ".csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        
        # Save DataFrame to temporary JSON file
        json_path = os.path.join(temp_dir, os.path.splitext(file.filename)[0] + ".json")
        df.to_json(json_path, orient="records", force_ascii=False, indent=2)
        
        # Clean up temporary upload file
        os.remove(temp_path)
        
        return jsonify({
            "success": True,
            "message": f"Successfully parsed {len(feed.entity)} entities",
            "feed_info": response_data["feed_info"],
            "csv_ready": True,
            "json_ready": True,
            "entity_counts": {
                "alerts": sum(1 for e in response_data["entities"] if e["type"] == "alert"),
                "trip_updates": sum(1 for e in response_data["entities"] if e["type"] == "trip_update"),
                "vehicle_positions": sum(1 for e in response_data["entities"] if e["type"] == "vehicle")
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/download/<format_type>')
def download_file(format_type):
    filename = request.args.get('filename', '')
    if not filename:
        return jsonify({"error": "No filename provided"}), 400
    
    temp_dir = tempfile.gettempdir()
    
    if format_type == 'csv':
        file_path = os.path.join(temp_dir, filename + ".csv")
        mime_type = 'text/csv'
        download_name = filename + ".csv"
    elif format_type == 'json':
        file_path = os.path.join(temp_dir, filename + ".json")
        mime_type = 'application/json'
        download_name = filename + ".json"
    else:
        return jsonify({"error": "Invalid format"}), 400
    
    if not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404
    
    return send_file(file_path, 
                     mimetype=mime_type,
                     as_attachment=True,
                     download_name=download_name)

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
        "Active Periods": active_periods,
        "Informed Entities": informed_entities,
        "Cause": cause,
        "Effect": effect,
        "Header Text": header_text,
        "Description Text": description_text
    }

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

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Create index.html template
    with open('templates/index.html', 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GTFS Realtime Parser</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            color: #333;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1, h2, h3 {
            color: #2c3e50;
        }
        .upload-container {
            margin-bottom: 20px;
            padding: 20px;
            border: 2px dashed #ccc;
            border-radius: 5px;
            text-align: center;
        }
        .upload-container.active {
            border-color: #3498db;
        }
        .button-group {
            margin: 20px 0;
        }
        button {
            background-color: #3498db;
            color: white;
            border: none;
            padding: 10px 15px;
            margin-right: 10px;
            border-radius: 4px;
            cursor: pointer;
        }
        button:hover {
            background-color: #2980b9;
        }
        button:disabled {
            background-color: #95a5a6;
            cursor: not-allowed;
        }
        .result-container {
            margin-top: 20px;
            display: none;
        }
        .loading {
            text-align: center;
            margin: 20px 0;
            display: none;
        }
        .spinner {
            border: 4px solid rgba(0, 0, 0, 0.1);
            border-left-color: #3498db;
            border-radius: 50%;
            width: 30px;
            height: 30px;
            animation: spin 1s linear infinite;
            display: inline-block;
            vertical-align: middle;
            margin-right: 10px;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        .error {
            color: #e74c3c;
            padding: 10px;
            border: 1px solid #e74c3c;
            border-radius: 4px;
            margin: 20px 0;
            display: none;
        }
        .info-card {
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
        }
        .success {
            background-color: #d4edda;
            color: #155724;
            padding: 10px;
            border-radius: 4px;
            margin-bottom: 20px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>GTFS Realtime Parser</h1>
        
        <div class="info-card">
            <h3>About This Tool</h3>
            <p>This tool parses GTFS Realtime Protocol Buffer (.pb) files and extracts the data into CSV and JSON formats.</p>
            <p>Upload a .pb file to get started.</p>
        </div>
        
        <div class="upload-container" id="drop-area">
            <p>Drop your GTFS Realtime (.pb) file here</p>
            <p>or</p>
            <input type="file" id="fileInput" accept=".pb" style="display: none;">
            <button id="browseButton">Browse Files</button>
            <p id="file-name"></p>
        </div>
        
        <div class="button-group">
            <button id="parseButton" disabled>Parse GTFS Data</button>
        </div>
        
        <div id="loading" class="loading">
            <div class="spinner"></div>
            <span>Processing GTFS data...</span>
        </div>
        
        <div id="error-container" class="error"></div>
        
        <div id="result-container" class="result-container">
            <div class="success" id="success-message"></div>
            
            <div class="info-card">
                <h3>Feed Information</h3>
                <p id="feed-version">Version: </p>
                <p id="feed-timestamp">Timestamp: </p>
                <p id="entity-count">Entities: </p>
                
                <h4>Entity Types</h4>
                <p id="alert-count">Alerts: </p>
                <p id="trip-count">Trip Updates: </p>
                <p id="vehicle-count">Vehicle Positions: </p>
            </div>
            
            <div class="button-group">
                <button id="downloadCsvButton">Download CSV</button>
                <button id="downloadJsonButton">Download JSON</button>
            </div>
        </div>
    </div>
    
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const dropArea = document.getElementById('drop-area');
            const fileInput = document.getElementById('fileInput');
            const browseButton = document.getElementById('browseButton');
            const parseButton = document.getElementById('parseButton');
            const downloadCsvButton = document.getElementById('downloadCsvButton');
            const downloadJsonButton = document.getElementById('downloadJsonButton');
            const fileNameDisplay = document.getElementById('file-name');
            const loadingIndicator = document.getElementById('loading');
            const errorContainer = document.getElementById('error-container');
            const resultContainer = document.getElementById('result-container');
            const successMessage = document.getElementById('success-message');
            const feedVersion = document.getElementById('feed-version');
            const feedTimestamp = document.getElementById('feed-timestamp');
            const entityCount = document.getElementById('entity-count');
            const alertCount = document.getElementById('alert-count');
            const tripCount = document.getElementById('trip-count');
            const vehicleCount = document.getElementById('vehicle-count');
            
            // Store the file name without extension for download
            let baseFileName = '';
            
            // Drag and drop functionality
            ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
                dropArea.addEventListener(eventName, preventDefaults, false);
            });

            function preventDefaults(e) {
                e.preventDefault();
                e.stopPropagation();
            }

            ['dragenter', 'dragover'].forEach(eventName => {
                dropArea.addEventListener(eventName, highlight, false);
            });

            ['dragleave', 'drop'].forEach(eventName => {
                dropArea.addEventListener(eventName, unhighlight, false);
            });

            function highlight() {
                dropArea.classList.add('active');
            }

            function unhighlight() {
                dropArea.classList.remove('active');
            }

            dropArea.addEventListener('drop', handleDrop, false);

            function handleDrop(e) {
                const dt = e.dataTransfer;
                const files = dt.files;
                if (files.length) {
                    handleFiles(files);
                }
            }

            function handleFiles(files) {
                const file = files[0];
                if (file && file.name.endsWith('.pb')) {
                    fileNameDisplay.textContent = `Selected file: ${file.name}`;
                    parseButton.disabled = false;
                    fileInput.files = files;
                    
                    // Store base file name for download
                    baseFileName = file.name.replace('.pb', '');
                } else {
                    fileNameDisplay.textContent = 'Please select a valid .pb file';
                    parseButton.disabled = true;
                }
            }

            browseButton.addEventListener('click', () => {
                fileInput.click();
            });

            fileInput.addEventListener('change', (e) => {
                if (fileInput.files.length) {
                    handleFiles(fileInput.files);
                }
            });

            // Parse GTFS Data
            parseButton.addEventListener('click', async () => {
                if (!fileInput.files.length) return;
                
                const file = fileInput.files[0];
                errorContainer.style.display = 'none';
                loadingIndicator.style.display = 'flex';
                resultContainer.style.display = 'none';
                
                try {
                    const formData = new FormData();
                    formData.append('file', file);
                    
                    const response = await fetch('/upload', {
                        method: 'POST',
                        body: formData
                    });
                    
                    const data = await response.json();
                    
                    if (response.ok) {
                        // Update UI with the result
                        successMessage.textContent = data.message;
                        feedVersion.textContent = `Version: ${data.feed_info.version}`;
                        feedTimestamp.textContent = `Timestamp: ${data.feed_info.timestamp ? new Date(data.feed_info.timestamp).toLocaleString() : 'Not available'}`;
                        entityCount.textContent = `Entities: ${data.feed_info.entity_count}`;
                        
                        alertCount.textContent = `Alerts: ${data.entity_counts.alerts}`;
                        tripCount.textContent = `Trip Updates: ${data.entity_counts.trip_updates}`;
                        vehicleCount.textContent = `Vehicle Positions: ${data.entity_counts.vehicle_positions}`;
                        
                        // Show the result container
                        resultContainer.style.display = 'block';
                    } else {
                        throw new Error(data.error || 'Error parsing GTFS data');
                    }
                } catch (error) {
                    console.error('Error:', error);
                    errorContainer.textContent = error.message;
                    errorContainer.style.display = 'block';
                } finally {
                    loadingIndicator.style.display = 'none';
                }
            });
            
            // Download buttons
            downloadCsvButton.addEventListener('click', () => {
                window.location.href = `/download/csv?filename=${encodeURIComponent(baseFileName)}`;
            });
            
            downloadJsonButton.addEventListener('click', () => {
                window.location.href = `/download/json?filename=${encodeURIComponent(baseFileName)}`;
            });
        });
    </script>
</body>
</html>''')
    
    app.run(debug=True, host='0.0.0.0', port=5000)
