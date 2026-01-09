-- 1. Création des bases de données (si elles n'existent pas)
CREATE DATABASE mlflow;
CREATE DATABASE airlines;

-- 2. On se connecte à la base métier
\c airlines;

-- 3. Création des tables
CREATE TABLE IF NOT EXISTS flight_static (
    callsign VARCHAR(10) PRIMARY KEY,
    airline_name VARCHAR(100),
    origin_code VARCHAR(3),
    destination_code VARCHAR(3),
    commercial_flight BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_static_airline ON flight_static(airline_name);
CREATE INDEX IF NOT EXISTS idx_static_origin ON flight_static(origin_code);
CREATE INDEX IF NOT EXISTS idx_static_dest ON flight_static(destination_code);

CREATE TABLE IF NOT EXISTS flight_dynamic (
    callsign VARCHAR(10) NOT NULL,
    icao24 VARCHAR(10) NOT NULL,
    flight_date DATE NOT NULL,
    departure_scheduled TIME NOT NULL,
    departure_actual TIME,
    arrival_scheduled TIME,
    arrival_actual TIME,
    status VARCHAR(15) NOT NULL, -- Augmenté de 10 à 15 pour supporter "just landed"
    last_update TIMESTAMPTZ DEFAULT NOW(), -- Changé en TIMESTAMPTZ pour la précision du delta 20min
    unique_key TEXT NOT NULL,
    CONSTRAINT pk_flight_dynamic PRIMARY KEY (unique_key)
);

CREATE INDEX IF NOT EXISTS idx_dynamic_callsign ON flight_dynamic(callsign);
CREATE INDEX IF NOT EXISTS idx_dynamic_departure ON flight_dynamic(departure_scheduled);
CREATE INDEX IF NOT EXISTS idx_dynamic_arrival ON flight_dynamic(arrival_scheduled);
-- Cet index est crucial pour la rapidité de la méthode needs_refresh
CREATE INDEX IF NOT EXISTS idx_dynamic_callsign_update ON flight_dynamic(callsign, last_update DESC);

CREATE TABLE IF NOT EXISTS live_data (
    indice SERIAL,
    request_id UUID NOT NULL,
    callsign VARCHAR(10) NOT NULL,
    icao24 VARCHAR(10) NOT NULL,
    flight_date DATE NOT NULL,
    departure_scheduled TIME NOT NULL,
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
    unique_key TEXT NOT NULL,
    CONSTRAINT pk_live_data PRIMARY KEY (request_id, unique_key),
    CONSTRAINT fk_live_dynamic FOREIGN KEY(unique_key) 
        REFERENCES flight_dynamic(unique_key)
);

CREATE INDEX IF NOT EXISTS idx_live_indice ON live_data(indice DESC);
CREATE INDEX IF NOT EXISTS idx_live_callsign ON live_data(callsign);
CREATE INDEX IF NOT EXISTS idx_live_icao24 ON live_data(icao24);
CREATE INDEX IF NOT EXISTS idx_live_unique_key ON live_data(unique_key);