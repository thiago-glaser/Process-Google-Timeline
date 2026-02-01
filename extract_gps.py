#!/usr/bin/env python3
"""
Extract GPS coordinates from Google Timeline JSON file
Converts Timeline.json to coordinates.csv
"""

import json
import csv
from pathlib import Path
from datetime import datetime, timezone
import re

def parse_latlng(latlng_str):
    """Parse latitude and longitude from string like '49.9033985°, -97.0022461°'"""
    try:
        # Remove degree symbols and split
        cleaned = latlng_str.replace('°', '').strip()
        parts = cleaned.split(',')
        lat = float(parts[0].strip())
        lng = float(parts[1].strip())
        return lat, lng
    except (ValueError, IndexError):
        return None, None

def convert_to_utc(timestamp_str):
    """Convert timezone-aware timestamp to UTC"""
    try:
        # Parse the timestamp with timezone
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        # Convert to UTC
        utc_dt = dt.astimezone(timezone.utc)
        # Return in ISO 8601 format with Z suffix
        return utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    except Exception as e:
        print(f"Error converting timestamp {timestamp_str}: {e}")
        return timestamp_str

def extract_gps_coordinates(json_file, output_csv):
    """Extract GPS coordinates from Timeline.json"""
    
    print(f"Reading {json_file}...")
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error reading JSON: {e}")
        return 0
    
    coordinates = []
    raw_signals = data.get('rawSignals', [])
    
    print(f"Processing {len(raw_signals)} raw signals...")
    
    for idx, signal in enumerate(raw_signals):
        if idx % 50000 == 0 and idx > 0:
            print(f"  Processed {idx} records...")
        
        position = signal.get('position', {})
        
        latlng = position.get('LatLng', '')
        timestamp = position.get('timestamp', '')
        accuracy = position.get('accuracyMeters', '')
        altitude = signal.get('altitude', '')
        
        if latlng and timestamp:
            lat, lng = parse_latlng(latlng)
            
            if lat is not None and lng is not None:
                # Convert to UTC
                utc_timestamp = convert_to_utc(timestamp)
                
                # Handle altitude - extract numeric value if present
                altitude_value = None
                if isinstance(altitude, (int, float)):
                    altitude_value = altitude
                elif altitude:
                    try:
                        altitude_value = float(str(altitude).split()[0])
                    except:
                        altitude_value = None
                
                coordinates.append({
                    'datetime': utc_timestamp,
                    'latitude': lat,
                    'longitude': lng,
                    'altitude': altitude_value if altitude_value is not None else ''
                })
    
    # Extract from semanticSegments (timelinePath)
    semantic_segments = data.get('semanticSegments', [])
    print(f"Processing {len(semantic_segments)} semantic segments...")
    
    for idx, segment in enumerate(semantic_segments):
        if idx % 10000 == 0 and idx > 0:
            print(f"  Processed {idx} segments...")
        
        timeline_path = segment.get('timelinePath', [])
        
        for point_data in timeline_path:
            point = point_data.get('point', '')
            timestamp = point_data.get('time', '')
            
            if point and timestamp:
                lat, lng = parse_latlng(point)
                
                if lat is not None and lng is not None:
                    # Convert to UTC
                    utc_timestamp = convert_to_utc(timestamp)
                    
                    coordinates.append({
                        'datetime': utc_timestamp,
                        'latitude': lat,
                        'longitude': lng,
                        'altitude': ''
                    })
    
    # Remove duplicates while preserving order
    seen = set()
    unique_coordinates = []
    for coord in coordinates:
        key = (coord['datetime'], coord['latitude'], coord['longitude'])
        if key not in seen:
            seen.add(key)
            unique_coordinates.append(coord)
    
    # Sort by datetime
    unique_coordinates.sort(key=lambda x: x['datetime'])
    
    print(f"\nWriting {len(unique_coordinates)} unique coordinates to {output_csv}...")
    
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['datetime', 'latitude', 'longitude', 'altitude'])
        writer.writeheader()
        writer.writerows(unique_coordinates)
    
    print(f"SUCCESS: Extracted {len(unique_coordinates)} unique GPS coordinates")
    print(f"SUCCESS: Saved to: {output_csv}")
    
    return len(unique_coordinates)

if __name__ == "__main__":
    import sys
    
    json_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("Timeline.json")
    
    if not json_path.exists():
        print(f"Error: {json_path} not found")
        sys.exit(1)
    
    output_csv = json_path.parent / "coordinates.csv"
    extract_gps_coordinates(json_path, output_csv)
