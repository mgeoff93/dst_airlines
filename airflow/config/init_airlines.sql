CREATE DATABASE airlines;

\c airlines;

CREATE TABLE IF NOT EXISTS flight_static (
	callsign VARCHAR(10) PRIMARY KEY,
	airline_name VARCHAR(100),
	origin_code VARCHAR(3),
	destination_code VARCHAR(3),
	origin_airport VARCHAR(100),
	destination_airport VARCHAR(100),
	origin_city VARCHAR(100),
	destination_city VARCHAR(100),
	commercial_flight BOOLEAN
);

CREATE INDEX idx_static_airline ON flight_static(airline_name);
CREATE INDEX idx_static_origin ON flight_static(origin_code);
CREATE INDEX idx_static_dest ON flight_static(destination_code);

CREATE TABLE flight_dynamic (
	callsign VARCHAR(10) NOT NULL,
	icao24 VARCHAR(10) NOT NULL,
	flight_date DATE NOT NULL,
	departure_scheduled VARCHAR(5) NOT NULL,
	departure_actual VARCHAR(5),
	arrival_scheduled VARCHAR(5),
	arrival_actual VARCHAR(5),
	status VARCHAR(10) NOT NULL,
	last_update TIMESTAMP DEFAULT NOW(),
	unique_key VARCHAR(64) NOT NULL,

	CONSTRAINT pk_flight_dynamic PRIMARY KEY (unique_key)
);

CREATE INDEX idx_dynamic_callsign ON flight_dynamic(callsign);
CREATE INDEX idx_dynamic_departure ON flight_dynamic(departure_scheduled);
CREATE INDEX idx_dynamic_arrival ON flight_dynamic(arrival_scheduled);
CREATE INDEX idx_dynamic_callsign_update
	ON flight_dynamic(callsign, last_update DESC);

CREATE TABLE IF NOT EXISTS live_data (
	request_id UUID NOT NULL,
	callsign VARCHAR(10) NOT NULL,
	icao24 VARCHAR(10) NOT NULL,
	flight_date DATE NOT NULL,
	departure_scheduled VARCHAR(5) NOT NULL,
	longitude DOUBLE PRECISION,
	latitude DOUBLE PRECISION,
	baro_altitude DOUBLE PRECISION,
	geo_altitude DOUBLE PRECISION, 
	on_ground BOOLEAN,
	velocity DOUBLE PRECISION,
	vertical_rate DOUBLE PRECISION,
	temperature DOUBLE PRECISION,
	wind_speed DOUBLE PRECISION,
	gust_speed DOUBLE PRECISION,
	visibility DOUBLE PRECISION,
	cloud_coverage DOUBLE PRECISION,
	rain DOUBLE PRECISION,
	global_condition VARCHAR(100),

	CONSTRAINT pk_live_data PRIMARY KEY (request_id, callsign, icao24, flight_date, departure_scheduled),
	CONSTRAINT fk_live_dynamic FOREIGN KEY(callsign, icao24, flight_date, departure_scheduled)
		REFERENCES flight_dynamic(callsign, icao24, flight_date, departure_scheduled)
);

CREATE INDEX idx_live_callsign ON live_data(callsign);
CREATE INDEX idx_live_icao24 ON live_data(icao24);