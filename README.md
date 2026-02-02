# Airlines
```
docker exec -it dst_airlines-airflow-scheduler-1 airflow dags reserialize
docker compose run --rm airflow-cli bash
```

-- Optionnel : vide les positions live pour repartir sur un dashboard propre
TRUNCATE TABLE live_data;
-- Marque les vieux vols comme arrivés pour que le triage les ignore
UPDATE flight_dynamic SET status = 'arrived' WHERE status IN ('en route', 'departing');

docker compose exec airflow-scheduler airflow dags delete etl

```
docker compose exec postgres psql -U airflow -d airlines
select * from live_data;
# select count(distinct timestamp) from live_data;
```

```
COPY opensky_flights TO '/tmp/live_data.csv' DELIMITER ',' CSV HEADER;
```

```
>>> a = {item["callsign"] for item in live_rows}
>>> b = {item["callsign"] for item in static_rows}
>>> a-b # ou b-a
```

```
docker compose down --volumes --rmi all
docker compose up --build -d
```

```
http://localhost:8080/
http://localhost:8000/docs
```