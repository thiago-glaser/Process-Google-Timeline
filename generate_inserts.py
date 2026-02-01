#!/usr/bin/env python3
"""
Generate Oracle INSERT statements from coordinates.csv
Creates both single and batch INSERT statements
"""

import csv
from pathlib import Path

def generate_inserts(csv_file, output_sql, batch_size=100):
    """Generate Oracle INSERT statements from CSV"""
    
    print(f"Reading {csv_file}...")
    
    inserts = []
    row_count = 0
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            row_count += 1
            
            datetime_utc = row['datetime']
            latitude = row['latitude']
            longitude = row['longitude']
            altitude = row['altitude'] if row['altitude'] else 'NULL'
            
            # Escape single quotes in datetime string
            datetime_utc = datetime_utc.replace("'", "''")
            
            # Handle altitude as NULL or as a number
            if altitude == 'NULL':
                altitude_clause = 'NULL'
            else:
                altitude_clause = altitude
            
            # Create INSERT statement - convert Z to +00:00 for Oracle
            datetime_utc_converted = datetime_utc.replace('Z', '+00:00')
            insert_stmt = (
                f"INSERT INTO GPS_COORDINATES (datetime_utc, latitude, longitude, altitude_meters) "
                f"VALUES (TO_TIMESTAMP_TZ('{datetime_utc_converted}', 'YYYY-MM-DD\"T\"HH24:MI:SSTZH:TZM'), "
                f"{latitude}, {longitude}, {altitude_clause});"
            )
            
            inserts.append(insert_stmt)
    
    # Write to SQL file
    print(f"Writing {len(inserts)} INSERT statements to {output_sql}...\n")
    
    with open(output_sql, 'w', encoding='utf-8') as f:
        # Add header comments
        f.write("-- Oracle INSERT statements for GPS_COORDINATES table\n")
        f.write(f"-- Total records: {len(inserts)}\n")
        f.write(f"-- Generated from coordinates.csv\n")
        f.write(f"-- Execute with: sqlplus user/password @{output_sql}\n\n")
        
        f.write("-- Optional: Disable constraints for faster loading\n")
        f.write("-- ALTER TABLE GPS_COORDINATES DISABLE ALL TRIGGERS;\n\n")
        
        f.write("-- Start transaction\n")
        f.write("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\n\n")
        
        f.write("-- Individual INSERT statements\n")
        f.write("BEGIN\n")
        
        for idx, stmt in enumerate(inserts, 1):
            f.write(f"  {stmt}\n")
            f.write(f"  COMMIT; -- Committed record {idx}\n")
        f.write("END;\n")
        f.write("/\n\n")
        
        f.write("-- Verify record count\n")
        f.write("SELECT COUNT(*) as total_records FROM GPS_COORDINATES;\n\n")
        
        f.write("-- Optional: Re-enable constraints\n")
        f.write("-- ALTER TABLE GPS_COORDINATES ENABLE ALL TRIGGERS;\n")
    
    print(f"Generated {len(inserts)} INSERT statements")
    print(f"Saved to: {output_sql}")
    
    return len(inserts)

def generate_batch_inserts(csv_file, output_sql, batch_size=1000):
    """Generate batch INSERT statements (more efficient)"""
    
    print(f"\nGenerating batch INSERT statements (batch size: {batch_size})...\n")
    
    inserts = []
    row_count = 0
    batch = []
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            row_count += 1
            
            datetime_utc = row['datetime'].replace("'", "''")
            latitude = row['latitude']
            longitude = row['longitude']
            altitude = row['altitude'] if row['altitude'] else 'NULL'
            
            if altitude != 'NULL':
                batch.append(
                    f"({datetime_utc!r}, {latitude}, {longitude}, {altitude})"
                )
            else:
                batch.append(
                    f"({datetime_utc!r}, {latitude}, {longitude}, NULL)"
                )
            
            # Write batch when size reached
            if len(batch) >= batch_size:
                inserts.append(batch)
                batch = []
    
    # Add remaining records
    if batch:
        inserts.append(batch)
    
    # Write to SQL file
    print(f"Writing {row_count} records in {len(inserts)} batch INSERT statements...\n")
    
    with open(output_sql, 'w', encoding='utf-8') as f:
        # Add header comments
        f.write("-- Oracle BATCH INSERT statements for GPS_COORDINATES table\n")
        f.write(f"-- Total records: {row_count}\n")
        f.write(f"-- Batch size: {batch_size}\n")
        f.write(f"-- Generated from coordinates.csv\n")
        f.write(f"-- Execute with: sqlplus user/password @{output_sql}\n\n")
        
        f.write("SET TIMING ON;\n")
        f.write("SET FEEDBACK ON;\n\n")
        
        for batch_num, batch_data in enumerate(inserts, 1):
            f.write(f"-- Batch {batch_num} ({len(batch_data)} records)\n")
            f.write("INSERT INTO GPS_COORDINATES (datetime_utc, latitude, longitude, altitude_meters)\n")
            f.write("SELECT * FROM (\n")
            f.write("  SELECT TO_TIMESTAMP_TZ(col1, 'YYYY-MM-DDTHH24:MI:SSXFF TZR'), col2, col3, col4 FROM (\n")
            
            f.write("    VALUES ")
            f.write(",\n           ".join(batch_data))
            f.write("\n  )\n")
            f.write(");\n")
            f.write("COMMIT;\n\n")
        
        f.write("-- Verify record count\n")
        f.write("SELECT COUNT(*) as total_records FROM GPS_COORDINATES;\n")
    
    print(f"Generated {len(inserts)} batches with {row_count} total records")
    print(f"Saved to: {output_sql}")
    
    return row_count

if __name__ == "__main__":
    import sys
    
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("coordinates.csv")
    
    if not csv_path.exists():
        print(f"Error: {csv_path} not found")
        sys.exit(1)
    
    # Generate individual INSERT statements
    output_individual = csv_path.parent / "insert_coordinates.sql"
    generate_inserts(csv_path, output_individual, batch_size=100)
    
    # Generate batch INSERT statements (more efficient)
    output_batch = csv_path.parent / "insert_coordinates_batch.sql"
    generate_batch_inserts(csv_path, output_batch, batch_size=1000)
    
    print("\n" + "="*60)
    print("Two SQL files generated:")
    print(f"  1. {output_individual.name} - Individual inserts with commits every record")
    print(f"  2. {output_batch.name} - Batch inserts (more efficient)")
    print("\nUse insert_coordinates_batch.sql for faster loading!")
    print("="*60)
