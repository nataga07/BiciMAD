"""
Autoras: Claudia Casado Poyatos
         Natalia García Domínguez
         Olga Rodríguez Acevedo 
"""

"""
ESTUDIO DE LA DURACIÓN MEDIA DE LOS VIAJES REALIZADOS Y DE LAS ESTACIONES (TANTO 
DE SALIDA COMO DE LLEGADA) MÁS Y MENOS FRECUENTADAS.
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Seleccionamos las variables a estudiar:
schema2 = StructType()\
    .add("travel_time", IntegerType(), False)\
    .add("idunplug_station", IntegerType(), False)\
    .add("idplug_station", IntegerType(), False)

nombre_fichero = '202105_movements.json'
df = spark.read.json(nombre_fichero)
df2 = spark.read.json(nombre_fichero, schema = schema2)
df2.show()

# Función que permite hallar la duración media de los viajes realizados:
def obtener_duracion_media(df):
    numero = df.count()
    total = df.rdd.map(lambda x: x['travel_time']).sum()
    media = (total / numero) / 60
    return media

# Calculamos la duración media de los viajes realizados durante el mes de mayo de 2021:
duracion_media = obtener_duracion_media(df2)
print('La duración media de los viajes realizados es de:', duracion_media, 'minutos.')

# Recogemos los datos de las estaciones de salida y llegada, las ordenamos siguiendo
# distintos criterios y contamos el número de trayectos que acoge cada estación:
df2_estacion_salida = df2.groupBy('idunplug_station').count().orderBy('count')
df2_estacion_llegada = df2.groupBy('idplug_station').count().orderBy('idplug_station')

# Funciones que permiten hallar el máximo y el mínimo, respectivamente, de 
# determinados conjuntos de datos:
def obtener_max(df, variable):
    max_ = df.rdd.map(lambda x: x['count']).max()
    df_max = df.filter(df['count'] == max_)
    df_max.show()
    maxi_frecuencia = df_max.rdd.map(lambda x: x[variable]).collect()
    return (max_, maxi_frecuencia)

def obtener_min(df, variable):
    min_ = df.rdd.map(lambda x: x['count']).min()
    df_min = df.filter(df['count'] == min_)
    df_min.show()
    mini_frecuencia = df_min.rdd.map(lambda x: x[variable]).collect()
    return (min_, mini_frecuencia)

# Hallamos las estaciones de salida más y menos frecuentadas:
(numero_max_tray_salida, estacion_mas_frec_salida) = obtener_max(df2_estacion_salida, 'idunplug_station')
print('La estación de salida más frecuentada es', estacion_mas_frec_salida[0], ', y', numero_max_tray_salida, ', el número de trayectos que ha acogido.')

(numero_min_tray_salida, estacion_menos_frec_salida) = obtener_min(df2_estacion_salida, 'idunplug_station')
print('La estación de salida menos frecuentada es', estacion_menos_frec_salida[0], ', y', numero_min_tray_salida, ', el número de trayectos que ha acogido.')

# Hallamos las estaciones de llegada más y menos frecuentadas:
(numero_max_tray_llegada, estacion_mas_frec_llegada) = obtener_max(df2_estacion_llegada, 'idplug_station')
print('La estación de llegada más frecuentada es', estacion_mas_frec_llegada[0], ', y', numero_max_tray_llegada, ', el número de trayectos que ha acogido.')

(numero_min_tray_llegada, estacion_menos_frec_llegada) = obtener_min(df2_estacion_llegada, 'idplug_station')
print('La estación de llegada menos frecuentada es', estacion_menos_frec_llegada[0], ', y', numero_min_tray_llegada, ', el número de trayectos que ha acogido.')