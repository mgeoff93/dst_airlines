-- 1. Création des bases de données
CREATE DATABASE mlflow;
CREATE DATABASE airlines;

-- 2. On se connecte à la base métier
\c airlines;

-- 3. Création des tables

-- TABLE: countries
CREATE TABLE IF NOT EXISTS countries (
    country_name TEXT PRIMARY KEY,
    country_code VARCHAR(5)
);

CREATE INDEX IF NOT EXISTS idx_countries_code ON countries(country_code);

-- TABLE: airports
CREATE TABLE IF NOT EXISTS airports (
    airport_code VARCHAR(10) PRIMARY KEY,
    airport_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    country_code VARCHAR(5)
);

-- TABLE: flight_static
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

-- TABLE: flight_dynamic
CREATE TABLE IF NOT EXISTS flight_dynamic (
    callsign VARCHAR(10) NOT NULL,
    icao24 VARCHAR(10) NOT NULL,
    flight_date DATE NOT NULL,
    departure_scheduled TIME NOT NULL,
    departure_actual TIME,
    arrival_scheduled TIME,
    arrival_actual TIME,
    status VARCHAR(15) NOT NULL,
    last_update TIMESTAMPTZ DEFAULT NOW(),
    unique_key TEXT NOT NULL,
    CONSTRAINT pk_flight_dynamic PRIMARY KEY (unique_key),
    CONSTRAINT fk_flight_dynamic_callsign FOREIGN KEY (callsign) 
        REFERENCES flight_static(callsign) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dynamic_callsign ON flight_dynamic(callsign);
CREATE INDEX IF NOT EXISTS idx_dynamic_departure ON flight_dynamic(departure_scheduled);
CREATE INDEX IF NOT EXISTS idx_dynamic_arrival ON flight_dynamic(arrival_scheduled);
CREATE INDEX IF NOT EXISTS idx_dynamic_callsign_update ON flight_dynamic(callsign, last_update DESC);

-- TABLE: live_data
CREATE TABLE IF NOT EXISTS live_data (
    indice SERIAL PRIMARY KEY,
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
    CONSTRAINT fk_live_data_unique_key FOREIGN KEY(unique_key) 
        REFERENCES flight_dynamic(unique_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_live_indice ON live_data(indice DESC);
CREATE INDEX IF NOT EXISTS idx_live_callsign ON live_data(callsign);
CREATE INDEX IF NOT EXISTS idx_live_icao24 ON live_data(icao24);
CREATE INDEX IF NOT EXISTS idx_live_unique_key ON live_data(unique_key);

-- 4. Import des données

-- Import Airports
CREATE TEMP TABLE temp_airports (
    code VARCHAR(10),
    name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    country_code VARCHAR(5),
    zone_code TEXT
);

COPY temp_airports FROM '/docker-entrypoint-initdb.d/airports.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';');

INSERT INTO airports (airport_code, airport_name, latitude, longitude, country_code)
SELECT code, name, latitude, longitude, country_code FROM temp_airports
ON CONFLICT DO NOTHING;

DROP TABLE temp_airports;

-- Import Countries
CREATE TEMP TABLE temp_countries (
    iso3 TEXT, name_en TEXT, name_fr TEXT, name_native TEXT, iso2 TEXT, borders TEXT, flag TEXT, latlng TEXT, wikidata TEXT, bologne TEXT, embassy TEXT, arab_world TEXT, central_europe_and_the_baltics TEXT, east_asia_pacific TEXT, euro_area TEXT, europe_central_asia TEXT, european_union TEXT, high_income TEXT, latin_america_caribbean TEXT, low_income TEXT, lower_middle_income TEXT, middle_east_north_africa TEXT, north_america TEXT, oecd_members TEXT, sub_saharan_africa TEXT, upper_middle_income TEXT, world TEXT, link TEXT, website TEXT, south_america TEXT, central_america_caraibes TEXT, continental_europe TEXT, asia_oceania TEXT, cocac TEXT, cf_mobility TEXT, idpaysage TEXT, curiexplore TEXT, idh_group TEXT, idh_group_countries TEXT, ue25 TEXT, ue27 TEXT, ue28 TEXT, g7 TEXT, g20 TEXT, geometry TEXT, ue_euro TEXT, oecd_obs TEXT, oecd_members_old TEXT, brics TEXT, code_num_3 TEXT, cog TEXT, code_pays_sise TEXT
);

COPY temp_countries FROM '/docker-entrypoint-initdb.d/countries.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL '');

INSERT INTO countries (country_name, country_code)
SELECT DISTINCT name_en, iso2 FROM temp_countries
WHERE name_en IS NOT NULL
ON CONFLICT DO NOTHING;

DROP TABLE temp_countries;