@echo off
SETLOCAL EnableDelayedExpansion

:: Configuration
SET OUTPUT_FILE=api\tests\seed_data.sql
SET LIMIT=5
SET CONTAINER_NAME=postgres
SET DB_USER=airflow
SET DB_NAME=airlines

echo [1/4] Preparation du fichier seed...
echo Seed Data coherent > %OUTPUT_FILE%
echo SET client_encoding = 'UTF8'; >> %OUTPUT_FILE%

echo [2/4] Creation du pivot de filtrage dans Docker...
docker exec -i %CONTAINER_NAME% psql -U %DB_USER% -d %DB_NAME% -c "DROP TABLE IF EXISTS tmp_seed_keys; CREATE TABLE tmp_seed_keys AS SELECT unique_key, callsign FROM flight_dynamic ORDER BY last_update DESC LIMIT %LIMIT%;"

echo [3/4] Extraction des donnees (INSERT INTO)...

:: A. STATIC
echo    - flight_static
docker exec -i %CONTAINER_NAME% pg_dump -U %DB_USER% -d %DB_NAME% -t flight_static --column-inserts --data-only --rows-per-insert 1 > temp_dump.sql
findstr /C:"INSERT INTO public.flight_static" temp_dump.sql >> %OUTPUT_FILE%

:: B. DYNAMIC
echo    - flight_dynamic
docker exec -i %CONTAINER_NAME% pg_dump -U %DB_USER% -d %DB_NAME% -t flight_dynamic --column-inserts --data-only --rows-per-insert 1 > temp_dump.sql
findstr /C:"INSERT INTO public.flight_dynamic" temp_dump.sql >> %OUTPUT_FILE%

:: C. LIVE
echo    - live_data
docker exec -i %CONTAINER_NAME% pg_dump -U %DB_USER% -d %DB_NAME% -t live_data --column-inserts --data-only --rows-per-insert 1 > temp_dump.sql
findstr /C:"INSERT INTO public.live_data" temp_dump.sql >> %OUTPUT_FILE%

echo [4/4] Nettoyage...
docker exec -i %CONTAINER_NAME% psql -U %DB_USER% -d %DB_NAME% -c "DROP TABLE tmp_seed_keys;"
del temp_dump.sql

echo Termine ! Fichier genere : %OUTPUT_FILE%
pause