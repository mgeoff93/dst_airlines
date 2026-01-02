# from fastapi import APIRouter, HTTPException
# from pydantic import BaseModel
# import pandas as pd
# from api.services.arrival_status_model import predict

# router = APIRouter(tags = ["ML Prediction"])

# class ArrivalStatusRequest(BaseModel):
# 	callsign: str
# 	airline_name: str
# 	origin_code: str
# 	destination_code: str
# 	icao24: str
# 	departure_status: str
# 	longitude: float
# 	latitude: float
# 	baro_altitude: float
# 	geo_altitude: float
# 	on_ground: bool
# 	velocity: float
# 	vertical_rate: float
# 	temperature: float
# 	wind_speed: float
# 	gust_speed: float
# 	visibility: float
# 	cloud_coverage: float
# 	rain: float
# 	global_condition: str

# @router.post("/predict/arrival_status")
# def predict_arrival_status(payload: ArrivalStatusRequest):
# 	try:
# 		# Convertir en dataframe
# 		df = pd.DataFrame([payload.dict()])
# 		result = predict(df)
# 		return {"arrival_status": result[0]}
# 	except Exception as e:
# 		raise HTTPException(status_code=500, detail=str(e))