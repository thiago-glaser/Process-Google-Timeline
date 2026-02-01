-- Oracle DDL for GPS Coordinates Table
-- Created for storing location history from Google Maps Timeline

CREATE TABLE GPS_COORDINATES (
    coordinate_id      NUMBER GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1) PRIMARY KEY,
    datetime_utc        TIMESTAMP WITH TIME ZONE NOT NULL,
    latitude            NUMBER(11, 8) NOT NULL,
    longitude           NUMBER(11, 8) NOT NULL,
    altitude_meters     NUMBER(10, 2),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_latitude CHECK (latitude >= -90 AND latitude <= 90),
    CONSTRAINT chk_longitude CHECK (longitude >= -180 AND longitude <= 180)
);

-- Create indexes for common queries
CREATE INDEX idx_datetime_utc ON GPS_COORDINATES(datetime_utc);
CREATE INDEX idx_coordinates ON GPS_COORDINATES(latitude, longitude);

-- Optional: Create a spatial index if using Oracle Spatial
-- CREATE INDEX idx_geo_location ON GPS_COORDINATES(mdsys.spatial_index) INDEXTYPE IS MDSYS.SPATIAL_INDEX;

-- Optional: Table comments
COMMENT ON TABLE GPS_COORDINATES IS 'GPS location coordinates extracted from Google Maps Timeline';
COMMENT ON COLUMN GPS_COORDINATES.coordinate_id IS 'Unique identifier for each coordinate record';
COMMENT ON COLUMN GPS_COORDINATES.datetime_utc IS 'Timestamp in UTC timezone';
COMMENT ON COLUMN GPS_COORDINATES.latitude IS 'Latitude in decimal degrees (-90 to 90)';
COMMENT ON COLUMN GPS_COORDINATES.longitude IS 'Longitude in decimal degrees (-180 to 180)';
COMMENT ON COLUMN GPS_COORDINATES.altitude_meters IS 'Altitude in meters above sea level (optional)';
