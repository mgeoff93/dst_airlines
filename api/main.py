from fastapi import FastAPI, HTTPException, Query
from api.postgres_client import PostgresClient
from api.routes.flights import router as flights_router
from datetime import date

app = FastAPI(
    title="DST Airlines API",
    description="API REST pour suivi des vols en temps réel",
    version="1.0.0"
)

app.include_router(flights_router)

db = PostgresClient()

# --- Healthcheck ---

@app.get("/health", tags=["Healthcheck"])
def health_check():
    try:
        db.query("SELECT 1;")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code = 503, detail = str(e))

# --- Static ---

@app.get("/static/all", tags=["Static Metadatas"])
def get_all_static(limit: int = Query(100, ge=1, le=1000)):
    sql = "SELECT * FROM flight_static LIMIT %s;"
    rows = db.query(sql, (limit,))
    return {"count": len(rows), "flights": rows}

@app.get("/static/{callsign}", tags=["Static Metadatas"])
def get_static_by_callsign(callsign: str):
    sql = "SELECT * FROM flight_static WHERE callsign = %s;"
    rows = db.query(sql, (callsign,))
    if not rows:
        raise HTTPException(status_code=404, detail="Flight not found")
    return rows[0]

@app.get("/static/{airlineName}", tags=["Static Metadatas"])
def get_static_by_airline(airline_name: str):
    sql = "SELECT * FROM flight_static WHERE airline_name = %s;"
    rows = db.query(sql, (airline_name,))
    return {"count": len(rows), "flights": rows}

@app.get("/static/from/{originCode}", tags=["Static Metadatas"])
def get_static_from_origin(origin_code: str):
    sql = "SELECT * FROM flight_static WHERE origin_code = %s;"
    rows = db.query(sql, (origin_code,))
    return {"count": len(rows), "flights": rows}

@app.get("/static/to/{destinationCode}", tags=["Static Metadatas"])
def get_static_to_destination(destination_code: str):
    sql = "SELECT * FROM flight_static WHERE destination_code = %s;"
    rows = db.query(sql, (destination_code,))
    return {"count": len(rows), "flights": rows}

# --- Dynamic ---

@app.get("/dynamic/all", tags=["Dynamic Metadatas"])
def get_all_dynamic(limit: int = Query(100, ge=1, le=1000)):
    sql = """
        SELECT *
        FROM flight_dynamic
        ORDER BY last_update DESC
        LIMIT %s;
    """
    rows = db.query(sql, (limit,))
    return {"count": len(rows), "flights": rows}

@app.get("/dynamic/latest/{callsign}", tags=["Dynamic Metadatas"])
def get_latest_dynamic_by_callsign(callsign: str):
    sql = """
        SELECT *
        FROM flight_dynamic
        WHERE callsign = %s
        ORDER BY flight_date DESC, departure_scheduled DESC
        LIMIT 1;
    """
    rows = db.query(sql, (callsign,))
    if not rows:
        raise HTTPException(status_code=404, detail="Flight not found")
    return rows[0]

@app.get("/dynamic/all/{callsign}", tags=["Dynamic Metadatas"])
def get_dynamic_history(callsign: str):
    sql = """
        SELECT *
        FROM flight_dynamic
        WHERE callsign = %s
        ORDER BY flight_date, departure_scheduled;
    """
    rows = db.query(sql, (callsign,))
    return {"count": len(rows), "flights": rows}

# --- Live ---

@app.get("/live/last/{icao24}", tags=["Live Data"])
def get_live_last(icao24: str):
    sql = """
        SELECT *
        FROM live_data
        WHERE icao24 = %s
        ORDER BY request_id DESC
        LIMIT 1;
    """
    rows = db.query(sql, (icao24,))
    if not rows:
        raise HTTPException(status_code=404, detail="Live data not found")
    return rows[0]

@app.get("/live/history", tags=["Live Data"])
def get_live_history(
    callsign: str,
    icao24: str,
    flight_date: date,
    departure_scheduled: str
):
    sql = """
        SELECT *
        FROM live_data
        WHERE callsign = %s
          AND icao24 = %s
          AND flight_date = %s
          AND departure_scheduled = %s
        ORDER BY request_id;
    """
    rows = db.query(sql, (callsign, icao24, flight_date, departure_scheduled))
    return {"count": len(rows), "data": rows}


# -------------------------------------------------------------------
# MERGED
# -------------------------------------------------------------------

@app.get("/flights/full/{callsign}", tags=["Merged"])
def get_full_flight(callsign: str):
    static = db.query(
        "SELECT * FROM flight_static WHERE callsign = %s;",
        (callsign,)
    )
    if not static:
        raise HTTPException(status_code=404, detail="Flight not found")

    dynamic = db.query(
        """
        SELECT *
        FROM flight_dynamic
        WHERE callsign = %s
        ORDER BY flight_date DESC, departure_scheduled DESC
        LIMIT 1;
        """,
        (callsign,)
    )

    live = None
    if dynamic:
        d = dynamic[0]
        live_rows = db.query(
            """
            SELECT *
            FROM live_data
            WHERE callsign = %s
              AND icao24 = %s
              AND flight_date = %s
              AND departure_scheduled = %s
            ORDER BY request_id DESC
            LIMIT 1;
            """,
            (
                d["callsign"],
                d["icao24"],
                d["flight_date"],
                d["departure_scheduled"]
            )
        )
        live = live_rows[0] if live_rows else None

    return {
        "callsign": callsign,
        "static": static[0],
        "dynamic": dynamic[0] if dynamic else None,
        "live": live
    }






# from fastapi import FastAPI, HTTPException, Query
# from postgres_client import PostgresClient
# from datetime import date

# app = FastAPI(
#     title="DST Airlines API",
#     description="API REST pour suivi des vols en temps réel",
#     version="1.0.0"
# )

# db = PostgresClient()

# # -------------------------------------------------------------------
# # Root & Health
# # -------------------------------------------------------------------

# @app.get("/", tags=["Root"])
# def root():
#     return {
#         "message": "DST Airlines API v1.0",
#         "docs": "/docs",
#         "health": "/health"
#     }

# @app.get("/health", tags=["Root"])
# def health_check():
#     try:
#         db.query("SELECT 1;")
#         return {"status": "healthy", "database": "connected"}
#     except Exception as e:
#         raise HTTPException(status_code=503, detail=str(e))


# # -------------------------------------------------------------------
# # STATIC
# # -------------------------------------------------------------------

# @app.get("/static", tags=["Static Metadatas"])
# def get_all_static(limit: int = Query(100, ge=1, le=1000)):
#     sql = "SELECT * FROM flight_static LIMIT %s;"
#     rows = db.query(sql, (limit,))
#     return {"count": len(rows), "flights": rows}

# @app.get("/static/{callsign}", tags=["Static Metadatas"])
# def get_static_by_callsign(callsign: str):
#     sql = "SELECT * FROM flight_static WHERE callsign = %s;"
#     rows = db.query(sql, (callsign,))
#     if not rows:
#         raise HTTPException(status_code=404, detail="Flight not found")
#     return rows[0]

# @app.get("/static/by-airline", tags=["Static Metadatas"])
# def get_static_by_airline(airline_name: str):
#     sql = "SELECT * FROM flight_static WHERE airline_name = %s;"
#     rows = db.query(sql, (airline_name,))
#     return {"count": len(rows), "flights": rows}

# @app.get("/static/from/{origin_code}", tags=["Static Metadatas"])
# def get_static_from_origin(origin_code: str):
#     sql = "SELECT * FROM flight_static WHERE origin_code = %s;"
#     rows = db.query(sql, (origin_code,))
#     return {"count": len(rows), "flights": rows}

# @app.get("/static/to/{destination_code}", tags=["Static Metadatas"])
# def get_static_to_destination(destination_code: str):
#     sql = "SELECT * FROM flight_static WHERE destination_code = %s;"
#     rows = db.query(sql, (destination_code,))
#     return {"count": len(rows), "flights": rows}


# # -------------------------------------------------------------------
# # DYNAMIC
# # -------------------------------------------------------------------

# @app.get("/dynamic", tags=["Dynamic Metadatas"])
# def get_all_dynamic(limit: int = Query(100, ge=1, le=1000)):
#     sql = """
#         SELECT *
#         FROM flight_dynamic
#         ORDER BY last_update DESC
#         LIMIT %s;
#     """
#     rows = db.query(sql, (limit,))
#     return {"count": len(rows), "flights": rows}

# @app.get("/dynamic/last/{callsign}", tags=["Dynamic Metadatas"])
# def get_last_dynamic_by_callsign(callsign: str):
#     sql = """
#         SELECT *
#         FROM flight_dynamic
#         WHERE callsign = %s
#         ORDER BY flight_date DESC, departure_scheduled DESC
#         LIMIT 1;
#     """
#     rows = db.query(sql, (callsign,))
#     if not rows:
#         raise HTTPException(status_code=404, detail="Flight not found")
#     return rows[0]

# @app.get("/dynamic/by-callsign/{callsign}", tags=["Dynamic Metadatas"])
# def get_dynamic_history(callsign: str):
#     sql = """
#         SELECT *
#         FROM flight_dynamic
#         WHERE callsign = %s
#         ORDER BY flight_date, departure_scheduled;
#     """
#     rows = db.query(sql, (callsign,))
#     return {"count": len(rows), "flights": rows}


# # -------------------------------------------------------------------
# # LIVE
# # -------------------------------------------------------------------

# @app.get("/live/last/{icao24}", tags=["Live Data"])
# def get_live_last(icao24: str):
#     sql = """
#         SELECT *
#         FROM live_data
#         WHERE icao24 = %s
#         ORDER BY request_id DESC
#         LIMIT 1;
#     """
#     rows = db.query(sql, (icao24,))
#     if not rows:
#         raise HTTPException(status_code=404, detail="Live data not found")
#     return rows[0]

# @app.get("/live/history", tags=["Live Data"])
# def get_live_history(
#     callsign: str,
#     icao24: str,
#     flight_date: date,
#     departure_scheduled: str
# ):
#     sql = """
#         SELECT *
#         FROM live_data
#         WHERE callsign = %s
#           AND icao24 = %s
#           AND flight_date = %s
#           AND departure_scheduled = %s
#         ORDER BY request_id;
#     """
#     rows = db.query(sql, (callsign, icao24, flight_date, departure_scheduled))
#     return {"count": len(rows), "data": rows}


# # -------------------------------------------------------------------
# # MERGED
# # -------------------------------------------------------------------

# @app.get("/flights/full/{callsign}", tags=["Merged"])
# def get_full_flight(callsign: str):
#     static = db.query(
#         "SELECT * FROM flight_static WHERE callsign = %s;",
#         (callsign,)
#     )
#     if not static:
#         raise HTTPException(status_code=404, detail="Flight not found")

#     dynamic = db.query(
#         """
#         SELECT *
#         FROM flight_dynamic
#         WHERE callsign = %s
#         ORDER BY flight_date DESC, departure_scheduled DESC
#         LIMIT 1;
#         """,
#         (callsign,)
#     )

#     live = None
#     if dynamic:
#         d = dynamic[0]
#         live_rows = db.query(
#             """
#             SELECT *
#             FROM live_data
#             WHERE callsign = %s
#               AND icao24 = %s
#               AND flight_date = %s
#               AND departure_scheduled = %s
#             ORDER BY request_id DESC
#             LIMIT 1;
#             """,
#             (
#                 d["callsign"],
#                 d["icao24"],
#                 d["flight_date"],
#                 d["departure_scheduled"]
#             )
#         )
#         live = live_rows[0] if live_rows else None

#     return {
#         "callsign": callsign,
#         "static": static[0],
#         "dynamic": dynamic[0] if dynamic else None,
#         "live": live
#     }