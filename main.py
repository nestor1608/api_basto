from support_api import filtro_finca, data_devices, select_data_by_date, dataframe_interview_vaca,setle_clean,df_gps,data_interview,select_data_by_dates
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from starlette.responses import RedirectResponse
import json


#Creo una instancia de FastAPI
app = FastAPI()

#---- PRESENTACIÓN--------

@app.get('/')
async def root():
    return RedirectResponse(url='/docs/')



#---------- Queries-----
#Primer consulta: Informacion propia de una finca.
@app.get("/informacion_por_finca/{nombre}")
async def informacion_por_finca(nombre: str):
    merge_data = data_interview(nombre)
    merge_data.point_ini= merge_data.point_ini.astype(str)
    merge_data.point_next= merge_data.point_next.astype(str)
    return JSONResponse(content= json.loads(merge_data.to_json()))


#Segunda consulta: Informacion propia de una finca en un periodo de tienpo.
@app.get("/informacion_por_un_periodo_por_finca/{nombre}/{fecha_init}/{fecha_fin}")
async def informacion_por_un_periodo_por_finca(nombre : str, fecha_init: str, fecha_fin : str):
    data_finca = setle_clean(nombre)
    df_gp = filtro_finca(df_gps,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    df_gp = select_data_by_dates(df_gp,fecha_init,fecha_fin)
    df_gp = data_interview(nombre,df_gp)
    df_gp.point_ini= df_gp.point_ini.astype(str)
    df_gp.point_next= df_gp.point_next.astype(str)
    return JSONResponse(content= json.loads(df_gp.to_json()))



#Tercera consulta: Toda la informacion de una vaca de un establecimiento
@app.get("/filtro_por_una_vaca_establecimiento/{nombre}/{id}")
async def filtro_por_una_vaca_establecimiento(nombre : str, id : str):
    data_finca = data_interview(nombre)
    df_gps = data_finca[data_finca.UUID==id]
    df_gps.point_ini= df_gps.point_ini.astype(str)
    df_gps.point_next= df_gps.point_next.astype(str)
    return JSONResponse(content= json.loads(df_gps.to_json()))



#Cuarta consulta: Toda la informacion de una vaca, en un establecimiento en una fecha
@app.get("/informacion_por_un_dia_una_vaca_por_finca/{nombre}/{id}/{fecha}")
async def informacion_por_un_dia_una_vaca_por_finca(nombre : str, id : str, fecha: str):
    data_finca = setle_clean(nombre)
    df_gp = filtro_finca(df_gps,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    df_gp = select_data_by_date(df_gp,fecha)
    df_gp = dataframe_interview_vaca(df_gp)
    df_gp.point_ini= df_gp.point_ini.astype(str)
    df_gp.point_next= df_gp.point_next.astype(str)
    return JSONResponse(content= json.loads(df_gp.to_json()))


#Quinta consulta: Toda la informacion de una vaca, en un establecimiento en un periodo de tiempo
@app.get("/informacion_por_un_periodo_una_vaca_por_finca/{nombre}/{id}/{fecha_init}/{fecha_fin}")
async def informacion_por_un_periodo_una_vaca_por_finca(nombre : str, id : str, fecha_init: str, fecha_fin : str):
    data_finca = setle_clean(nombre)
    df_gp = filtro_finca(df_gps,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    df_gp= data_devices(df_gp,id)
    df_gp = select_data_by_dates(df_gp,fecha_init,fecha_fin)
    df_gp = dataframe_interview_vaca(df_gp)
    df_gp.point_ini= df_gp.point_ini.astype(str)
    df_gp.point_next= df_gp.point_next.astype(str)
    return JSONResponse(content= json.loads(df_gp.to_json()))