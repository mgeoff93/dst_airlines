from fastapi import FastAPI, HTTPException, Query
from postgres_client import PostgresClient
from uuid import UUID

app = FastAPI(
    title="DST Airlines API",
    description="API REST pour suivi des vols en temps réel",
    version="1.0.0"
)

db = PostgresClient()

@app.get("/", tags=["Root"])
def root():
    return {
        "message": "DST Airlines API v1.0",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health", tags=["Root"])
def health_check():
    try:
        result = db.query("SELECT 1 as health;")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")

@app.get("/static", tags=["Flight Static"])
def get_all_static(limit: int = Query(100, ge=1, le=1000)):
    sql = "SELECT * FROM flight_static LIMIT %s;"
    result = db.query(sql, (limit,))
    return {"count": len(result), "flights": result}

@app.get("/static/{callsign}", tags=["Flight Static"])
def get_static_by_callsign(callsign: str):
    sql = "SELECT * FROM flight_static WHERE callsign = %s;"
    result = db.query(sql, (callsign,))
    if not result:
        raise HTTPException(status_code=404, detail=f"Flight '{callsign}' not found")
    return result[0]

@app.get("/static/by-airline", tags=["Flight Static"])
def get_static_by_airline(airline_name: str):
    sql = "SELECT * FROM flight_static WHERE airline_name = %s;"
    result = db.query(sql, (airline_name,))
    return {"count": len(result), "flights": result}

@app.get("/static/from/{origin_code}", tags=["Flight Static"])
def get_static_from_origin(origin_code: str):
    sql = "SELECT * FROM flight_static WHERE origin_code = %s;"
    result = db.query(sql, (origin_code,))
    return {"count": len(result), "flights": result}

@app.get("/static/to/{destination_code}", tags=["Flight Static"])
def get_static_to_destination(destination_code: str):
    sql = "SELECT * FROM flight_static WHERE destination_code = %s;"
    result = db.query(sql, (destination_code,))
    return {"count": len(result), "flights": result}

@app.get("/dynamic", tags=["Flight Dynamic"])
def get_all_dynamic(limit: int = Query(100, ge=1, le=1000)):
    sql = "SELECT * FROM flight_dynamic ORDER BY last_update DESC LIMIT %s;"
    result = db.query(sql, (limit,))
    return {"count": len(result), "flights": result}

@app.get("/dynamic/{flight_id}", tags=["Flight Dynamic"])
def get_dynamic_by_id(flight_id: UUID):
    sql = "SELECT * FROM flight_dynamic WHERE flight_id = %s;"
    result = db.query(sql, (str(flight_id),))
    if not result:
        raise HTTPException(status_code=404, detail="Dynamic flight not found")
    return result[0]

@app.get("/dynamic/by-callsign/{callsign}", tags=["Flight Dynamic"])
def get_dynamic_by_callsign(callsign: str):
    sql = "SELECT * FROM flight_dynamic WHERE callsign = %s ORDER BY last_update DESC;"
    result = db.query(sql, (callsign,))
    return {"count": len(result), "flights": result}

@app.get("/dynamic/last/{callsign}", tags=["Flight Dynamic"])
def get_dynamic_last(callsign: str):
    sql = """
        SELECT * FROM flight_dynamic 
        WHERE callsign = %s 
        ORDER BY last_update DESC 
        LIMIT 1;
    """
    result = db.query(sql, (callsign,))
    if not result:
        raise HTTPException(status_code=404, detail="Flight not found")
    return result[0]

@app.get("/live/last/{icao24}", tags=["Live Data"])
def get_live_last(icao24: str):
    sql = """
        SELECT * FROM live_data 
        WHERE icao24 = %s 
        ORDER BY request_id DESC 
        LIMIT 1;
    """
    result = db.query(sql, (icao24,))
    if not result:
        raise HTTPException(status_code=404, detail="Live data not found")
    return result[0]

@app.get("/live/history/{flight_id}", tags=["Live Data"])
def get_live_history(flight_id: UUID):
    sql = "SELECT * FROM live_data WHERE flight_id = %s ORDER BY request_id;"
    result = db.query(sql, (str(flight_id),))
    return {"count": len(result), "data": result}

@app.get("/flights/full/{callsign}", tags=["Merged"])
def get_full_flight(callsign: str):
    sql_static = "SELECT * FROM flight_static WHERE callsign = %s;"
    static = db.query(sql_static, (callsign,))
    if not static:
        raise HTTPException(status_code=404, detail=f"Flight '{callsign}' not found")
    static = static[0]
    
    sql_dynamic = """
        SELECT * FROM flight_dynamic
        WHERE callsign = %s
        ORDER BY last_update DESC 
        LIMIT 1;
    """
    dynamic = db.query(sql_dynamic, (callsign,))
    dynamic = dynamic[0] if dynamic else None
    
    live = None
    if dynamic:
        icao24 = dynamic.get("icao24")
        if icao24:
            sql_live = """
                SELECT * FROM live_data
                WHERE icao24 = %s
                ORDER BY request_id DESC 
                LIMIT 1;
            """
            live_data = db.query(sql_live, (icao24,))
            live = live_data[0] if live_data else None
    
    return {
        "callsign": callsign,
        "static": static,
        "dynamic": dynamic,
        "live": live
    }

@app.get("/flights/history/{callsign}", tags=["Merged"])
def get_full_history(callsign: str):
    sql_dynamic = "SELECT * FROM flight_dynamic WHERE callsign = %s ORDER BY last_update;"
    dynamics = db.query(sql_dynamic, (callsign,))
    
    if not dynamics:
        raise HTTPException(status_code=404, detail=f"No history for '{callsign}'")
    
    history = []
    for d in dynamics:
        flight_id = d["flight_id"]
        sql_live = "SELECT * FROM live_data WHERE flight_id = %s ORDER BY request_id;"
        live_records = db.query(sql_live, (str(flight_id),))
        history.append({
            "dynamic": d,
            "live": live_records
        })
    
    return {"callsign": callsign, "count": len(history), "history": history}
