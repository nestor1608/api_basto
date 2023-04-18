import pandas as pd
import geopandas as gpd
import pymongo
from pymongo import MongoClient
from typing import List
from shapely.geometry import Point
from geopy.distance import great_circle



# Crear una conexión a una instancia de MongoDB en la dirección 'localhost:27017'.
data_mongo: MongoClient = pymongo.MongoClient('mongodb+srv://brandon:brandon1@cluster0.tfvievv.mongodb.net/?retryWrites=true&w=majority')
# Seleccionar una base de datos existente o crear una nueva llamada 'test'.
db = data_mongo['test']
# Seleccionar una colección de la base de datos llamada 'datarows'.
rows = db['datarows']
data_row= rows.find({'dataRowType':'GPS'})
df_row=pd.json_normalize(data_row, sep='_')
df_row._id = df_row._id.astype(str)


# def filter_data_types(df_row: pd.DataFrame) -> tuple:
#     """
#     Se seleccionan las filas del DataFrame original donde el valor de la columna 'dataRowType' es 'GPS', 
#     'dataRowType' es 'BEACON' y 'dataRowType' es 'BATTERY' y se devuelve una tupla con los tres DataFrames filtrados
    
#     Parámetro:
#     -----------
#     - df_row: DataFrame que contiene los datos a filtrar.

#     Retorna:
#     --------
#     - Diferentes DataFrames, cada uno correspondiente al tipo de origen de dato indicado.
#     """

#     # Se cargan los valores en funcion de condicion de equivalencia.
#     data_gps = df_row[df_row.dataRowType == 'GPS']
#     data_beacon = df_row[df_row.dataRowType == 'BEACON']
#     data_battery = df_row[df_row.dataRowType == 'BATTERY']
#     return (data_gps, data_beacon, data_battery)

# data_gps,data_beacon,data_battery = filter_data_types(df_row)
# # Se seleccionan las columnas deseadas de cada futuro DataFrame .
df_gps=df_row[['UUID','dataRowType','createdAt','updatedAt','dataRowData_lat','dataRowData_lng','dataRowData_gpsAlt','dataRowData_gpsVel','dataRowData_gpsFixed']]
# df_bate=data_battery[['UUID','dataRowType','createdAt','updatedAt','dataRowData_timestamp','dataRowData_battery']]
# df_beacon=data_beacon[['UUID','dataRowType','createdAt','updatedAt','dataRowData_timestamp','dataRowData_mac','dataRowData_battery','dataRowData_temperature','dataRowData_rssi','dataRowData_accelerometer']]


def data_devices(data: pd.DataFrame, uuid: str) -> pd.DataFrame:
    """
    Filtra los datos de un DataFrame que corresponden a un dispositivo específico
    y elimina las filas con valores faltantes en la columna dataRowData_lat.

    Parámetros:
    -----------
    - data: DataFrame que contiene los datos a filtrar.
    - uuid: string que corresponde al identificador único del dispositivo a filtrar.

    Retorna:
    --------
    - Un DataFrame que contiene solo los datos del dispositivo especificado, sin valores faltantes en dataRowData_lat.
    """
    data = data[data.UUID == uuid]
    data.drop(data[data.dataRowData_lat.isna()].index, inplace=True)
    data.reset_index()
    return data

def gps_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Selecciona solo las columnas dataRowData_lat y dataRowData_lng de un DataFrame y
    elimina las filas con valores faltantes en alguna de estas columnas.

    Parámetros:
    -----------
    - data: DataFrame que contiene los datos de GPS a seleccionar.

    Retorna:
    --------
    - Un DataFrame que contiene solo las columnas dataRowData_lat y dataRowData_lng, sin valores faltantes.
    """
    gps = data[['dataRowData_lat', 'dataRowData_lng']]
    gps = gps.dropna()
    return gps

def perimetro_aprox(hectarea: float) -> float:
    """
    Calcula el perímetro aproximado de un terreno a partir de su área en hectáreas.
    
    Parámetros:
    -----------
    - hectarea: área del terreno en hectáreas
    
    Retorna:
    -----------
    - perim: perímetro aproximado del terreno en metros
    """
    hect = hectarea  # Asignamos el valor del parámetro hectarea a la variable hect
    lado = math.sqrt(hect) * 10  # Calculamos la longitud del lado de un cuadrado cuya área es igual a hect y multiplicamos por 10
    perim = lado * 4  # Calculamos el perímetro del cuadrado multiplicando la longitud del lado por 4

    return perim

def area_perimetro(data: pd.DataFrame, latitud: float, longitud: float, hectareas: float) -> gpd.GeoDataFrame:
    """
    Devuelve una GeoDataFrame con las geometrías de los terrenos que se encuentran en el perímetro de un círculo
    centrado en las coordenadas dadas y con un radio que corresponde al perímetro aproximado de un terreno
    de la misma área que se especifica.
    
    Parámetros:
    -----------
    - latitud: latitud del centro del círculo
    - longitud: longitud del centro del círculo
    - hectareas: área del terreno en hectáreas
    
    Retorna:
    -----------
    - on_perimetro: GeoDataFrame con las geometrías de los terrenos que se encuentran en el perímetro del círculo
    """
    gdf= gpd.GeoDataFrame(data,crs='EPSG:4326',geometry=gpd.points_from_xy(data.dataRowData_lng,data.dataRowData_lat))

    setle_lat = latitud # Asignamos el valor del parámetro latitud a la variable setle_lat
    setle_lng = longitud # Asignamos el valor del parámetro longitud a la variable setle_lng
    punto_referencia = Point(setle_lng, setle_lat) # Creamos un punto de referencia con las coordenadas setle_lat y setle_lng
    per_kilo = perimetro_aprox(hectareas) # Calculamos el perímetro en metros aproximado a partir del área en hectáreas
    circulo = punto_referencia.buffer(per_kilo/111.32) # Creamos un círculo con el radio igual al perímetro en metros, dividido entre 111.32 km, aproximando a 1 grado en el ecuador
    on_perimetro = gdf[gdf.geometry.within(circulo)] # Filtramos el GeoDataFrame gdf para obtener los puntos dentro del círculo creado anteriormente.
    return on_perimetro

def filtro_finca(dada: pd.DataFrame,lat: float, long: float, hect: int) -> pd.DataFrame:
    """
    Función que filtra el DataFrame resultante de la función 'area_perimetro' eliminando las filas con valores faltantes.

    Parametros:
    -----------
    - lat (float): latitud de la finca.
    - long (float): longitud de la finca.
    - hect (int): área en hectáreas de la finca.

    Retorno:
    -----------
    - df_finca (pd.DataFrame): DataFrame que contiene la información de la finca filtrada.
    """

    df_finca: pd.DataFrame = area_perimetro(dada,lat, long, hect)
    return df_finca

def select_data_by_date(df: pd.DataFrame, fecha: str) -> pd.DataFrame:
    """
    Selecciona las filas de un DataFrame correspondientes a una fecha específica.
    
    Parametros:
    - df: DataFrame de pandas que contiene la columna "createdAt".
    - fecha: Fecha en formato de cadena, en el formato 'YYYY-MM-DD'.
    
    Returno:
    - DataFrame de pandas que contiene solo las filas correspondientes a la fecha especificada.
    """
    
    # Convertir la columna "createdAt" en un objeto datetime
    df['createdAt'] = pd.to_datetime(df['createdAt'])

    # Seleccionar solo las filas correspondientes a la fecha especificada
    fecha_deseada = pd.to_datetime(fecha).date()
    nuevo_df = df.loc[df['createdAt'].dt.date == fecha_deseada]

    return nuevo_df

def dataframe_interview_vaca(data: pd.DataFrame) -> pd.DataFrame:
    """
    Función que procesa un DataFrame de datos de GPS para calcular la distancia recorrida, la velocidad promedio y el tiempo
    de recorrido entre cada par de puntos consecutivos. Además, agrega una columna con la relación de velocidad entre puntos 
    consecutivos.

    Parametros:
    -----------
    - DataFrame de datos de GPS con columnas 'createdAt', 'dataRowData_lat', 'dataRowData_lng' y 'dataRowData_gpsVel'
    
    Retorno:
    -----------
    - DataFrame con las columnas 'point_ini', 'point_next', 'interval_time', 'distancia', 'velocidad', 'tiempo' y 'charge_vel'
    """

    data_dis=[]
    data_vel=[]
    data_time=[]
    data_inter= []
    data_in=[]
    data_fin=[]
    data_alg=[]
    for i in range(0,data.shape[0]+1):
        try:
            dista_km= great_circle(tuple(data.iloc[i][['dataRowData_lat','dataRowData_lng']].values),tuple(data.iloc[i+1][['dataRowData_lat','dataRowData_lng']].values)).kilometers
            data_in.append(data.iloc[i][['createdAt']].values[0])
            data_fin.append(data.iloc[i+1][['createdAt']].values[0])
            interval= int(data.iloc[i+1][['createdAt']].values[0].strftime('%H')) - int(data.iloc[i][['createdAt']].values[0].strftime('%H'))
            data_inter.append(interval)
            if i == 0 : 
                data_var = data.iloc[i]['dataRowData_gpsVel']
                data_alg.append(data_var)
            else:
                data_var = data.iloc[i+1]['dataRowData_gpsVel']- data.iloc[i-1]['dataRowData_gpsVel']
                data_alg.append(data_var)
            if dista_km <= 8.:
                data_dis.append(round(dista_km,3))
            if data.iloc[i].dataRowData_gpsVel:
                data_vel.append(round(data.iloc[i].dataRowData_gpsVel,3))
                data_time.append(round(dista_km/data.iloc[i].dataRowData_gpsVel,3))
            else:
                data_time.append(round(dista_km/pd.Series(data_vel).mean().round(3),3))# les puede dar error si el array de velocidad esta vacio... toma el valor promedio de las velocidades que hay hasta el momento
        except IndexError:
            pass
    df = list(zip(data_in,data_fin,data_inter,data_dis,data_vel,data_time,data_alg))
    df = pd.DataFrame(df,columns=['point_ini','point_next' ,'interval_time','distancia','velocidad','tiempo','charge_vel']) 
    return df

def setle_clean(select):
    de= db['settlements']
    obj= de.find_one({'name':select})
    df_setle= pd.json_normalize(obj,sep='')
    df_setle['latitud_c']=df_setle.centralPoint.apply(lambda x: x[0]['lat'] if 'lat' in x[0] else None)
    df_setle['longitud_c']=df_setle.centralPoint.apply(lambda x: x[0]['lng'] if 'lng' in x[0] else None)
    setle_n = df_setle[['_id','hectares','registerNumber','headsCount','name','latitud_c','longitud_c']]
    return setle_n


def data_interview(nombre,data=df_row):
    data_finca = setle_clean(nombre)
    prueba = filtro_finca(data,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    vacas= prueba.UUID.unique()
    data_nuevo={}
    for i in vacas:
        data=data_devices(prueba,i)
        data_nuevo[i]=dataframe_interview_vaca(data)
    merge_data= pd.concat(data_nuevo.values(),keys=data_nuevo.keys())
    merge_data.reset_index(level=0,inplace=True)
    merge_data.rename(columns={'level_0':'UUID'},inplace=True)
    merge_data.reset_index(inplace=True)
    merge_data.set_index("UUID")
    merge_data.drop(columns="index",inplace=True)
    return merge_data


def select_data_by_dates(df: pd.DataFrame, fecha_init: str, fecha_fin : str) -> pd.DataFrame:
    """
    Selecciona las filas de un DataFrame correspondientes a una fecha específica.
    
    Parametros:
    - df: DataFrame de pandas que contiene la columna "createdAt".
    - fecha: Fecha en formato de cadena, en el formato 'YYYY-MM-DD'.
    
    Returno:
    - DataFrame de pandas que contiene solo las filas correspondientes a la fecha especificada.
    """
    
    # Convertir la columna "createdAt" en un objeto datetime
    df['createdAt'] = pd.to_datetime(df['createdAt'])

    # Seleccionar solo las filas correspondientes a la fecha especificada
    fecha_deseada1 = pd.to_datetime(fecha_init).date()
    fecha_deseada2 = pd.to_datetime(fecha_fin).date()

    nuevo_df = df[(df['createdAt'].dt.date >= fecha_deseada1) & (df['createdAt'].dt.date <= fecha_deseada2)]

    return nuevo_df