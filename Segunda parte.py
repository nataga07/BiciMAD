"""
Autoras: Claudia Casado Poyatos
         Natalia García Domínguez
         Olga Rodríguez Acevedo
"""

"""
ESTUDIO DE LA RELACIÓN ENTRE LA DURACIÓN MEDIA DE LOS VIAJES REALIZADOS Y
CADA UNA DE LAS SIGUIENTES VARIABLES:
    - RANGO DE EDAD 
    - FRANJA HORARIA
    - DÍA DE LA SEMANA 
"""

import json
# import matplotlib.pyplot as plt
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, TimestampType

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Seleccionamos las variables a estudiar:
schema3 = StructType()\
    .add("travel_time", IntegerType(), False)\
    .add("ageRange", IntegerType(), False)\
    .add("unplug_hourTime", TimestampType(), False)\
        
nombre_fichero = '202105_movements.json'
df3 = spark.read.json(nombre_fichero, schema = schema3)
df3.show()

# Función que permite hallar la duración media de los viajes realizados:
def obtener_duracion_media(df):
    numero = df.count()
    total = df.rdd.map(lambda x: x['travel_time']).sum()
    media = (total / numero) / 60
    return media

"""
-------- DURACIÓN MEDIA DE LOS VIAJES REALIZADOS VS RANGO DE EDAD --------
"""
# Analizamos las edades de los usuarios de BICIMAD y contamos el número de 
# trayectos realizados por viajeros de la misma edad:
df3_edad = df3.groupBy('ageRange').count().sort('ageRange')

resultados = []
for i in range(0, 7):
    df3_edad_i = df3.filter(df3['ageRange'] == i)
    duracion_media = obtener_duracion_media(df3_edad_i)
    resultados.append(duracion_media)
    print('La duración media de los viajes de BICIMAD en el rango de edad', i, 'es de:', duracion_media, 'minutos.')

# Histograma
# plt.bar(range(0,7), resultados)
# plt.title('Duración media por cada grupo de edad', fontsize = 15)
# plt.xlabel('Rango de edad')
# plt.ylabel('Minutos')

"""
-------- DURACIÓN MEDIA DE LOS VIAJES REALIZADOS VS FRANJA HORARIA --------
"""

# Esta función nos permite obtener la hora y el dia de la semana de todos los 
# viajes realizados:
def obtener_dia_hora(linea):
    datos = json.loads(linea)
    datos['unplug_hourTime'] = datetime.strptime(datos['unplug_hourTime'], "%Y-%m-%dT%H:%M:%SZ")
    datos['day'] = datos['unplug_hourTime'].isoweekday()
    datos['hour'] = datos['unplug_hourTime'].hour
    return datos

def obtener_franja_horaria(df, hora_p, hora_s):
    df_hora = df.filter(df['hour'] >= hora_p).filter(df['hour'] < hora_s).sort(df['hour'])
    return df_hora

rdd_dia_hora = sc.textFile(nombre_fichero).map(obtener_dia_hora)
df_dia_hora = spark.createDataFrame(rdd_dia_hora).drop('__id').drop('idplug_station').drop('idunplug_station').drop('idplug_base').drop('idunplug_base').drop('user_day_code').drop('user_type').drop('zip_code')
df_contar_hora = df_dia_hora.groupBy('hour').count()

f_horaria = [0, 7, 14, 20, 24]
franjas = ['madrugada', 'mañana', 'tarde', 'noche']

for i in range(len(f_horaria) - 1):
    df_franjas = obtener_franja_horaria(df_contar_hora, f_horaria[i], f_horaria[i+1])
    # Histograma:
    # viajes = df_franjas.select("count").rdd.flatMap(lambda x: x).collect()
    # plt.bar(range(f_horaria[i],f_horaria[i+1]), viajes)
    # plt.tittle('Viajes durante la ' + franjas[i], fontsize = 10)
    # plt.ylim(0, 33000)
    # plt.xlabel('Hora')
    # plt.show()

# Funciones que permiten hallar el máximo y el mínimo, respectivamente, de 
# determinados conjuntos de datos
def obtener_max(df, variable):
    max_ = df.rdd.map(lambda x: x['count']).max()
    df_max = df.filter(df['count'] == max_)
    df_max.show()
    maxi_frecuencia = df_max.rdd.map( lambda x: x[variable]).collect()
    return (max_, maxi_frecuencia)

def obtener_min(df, variable):
    min_ = df.rdd.map(lambda x: x['count']).min()
    df_min = df.filter(df['count'] == min_)
    df_min.show()
    mini_frecuencia = df_min.rdd.map(lambda x: x[variable]).collect()
    return (min_, mini_frecuencia)

(maximo, hora_max) = obtener_max(df_contar_hora, 'hour')
print('La mayor cantidad de trayectos se realizan a las:', hora_max[0], 'horas, y', maximo, 'es el total de viajes.')
(minimo, hora_min) = obtener_min(df_contar_hora, 'hour')
print('La menor cantidad de trayectos se realizan a las:', hora_min[0], 'horas, y', minimo, 'es el total de viajes.')

# Duración media de los viajes realizados según la franja horaria:
duracion_media_franja = []
for i in range(len(f_horaria) - 1):
    df_hora_i = obtener_franja_horaria(df_dia_hora, f_horaria[i], f_horaria[i+1])
    media_i = obtener_duracion_media(df_hora_i)
    duracion_media_franja.append(media_i)
    print('La duración media de los viajes realizados entre las', f_horaria[i], 'y las', f_horaria[i+1], 'es de:', media_i, 'minutos.')
    # Histograma:
    # plt.bar(franjas, duracion_media_franja)
    # plt.title('Duración media por cada franja horaria')
    # plt.ylabel('Horas')
    # plt.show()

# Podemos observar que la 'mañana' es la franja horaria en la que los viajes hechos
# son de mayor duración media. Por tanto, hacemos un estudio específico de la duración 
# de los trayectos realizados durante todas las horas que comprende dicha franja:
def obtener_horas_manana(df, hora):
    df_hora = df.filter(df['hour'] == hora)
    return df_hora

for i in range(f_horaria[1], f_horaria[2]):
    df_hora_i = obtener_horas_manana(df_dia_hora, i)
    duracion_media_hora_i = obtener_duracion_media(df_hora_i)
    print('La duración media de los viajes a las', i, 'horas es de:', duracion_media_hora_i, 'minutos.')

"""
-------- DURACIÓN MEDIA DE LOS VIAJES REALIZADOS VS DÍA DE LA SEMANA --------
"""

def obtener_franja_semanal(df, dia):
    if dia == 'laborable':
        df_semana = df.filter(df['day'] < 6).sort(df['day'])
    else:
        df_semana = df.filter(df['day'] > 5).sort(df['day'])
    return df_semana

df_contar_dia = df_dia_hora.groupBy('day').count()

# De lunes a viernes:
df_laborable = df_contar_dia.filter(df_contar_dia['day'] < 6).sort(df_contar_dia['day'])
df_laborable.show()

# De sábado a domingo:
df_finde = df_contar_dia.filter(df_contar_dia['day'] > 5).sort(df_contar_dia['day'])
df_finde.show()

#Duracion de los viajes en días laborables
df_LV = obtener_franja_semanal(df_dia_hora, 'laborable')
duracion_media_laborable = obtener_duracion_media(df_LV)
print('La duración media de los viajes de lunes a viernes es ', duracion_media_laborable, ' minutos.')

#Duracion de los viajes en fin de semana
df_SD = obtener_franja_semanal(df_dia_hora, 'finde')
duracion_media_finde = obtener_duracion_media(df_SD)
print('La duración media de los viajes los fines de semana es ',  duracion_media_finde, ' minutos