from pyspark.sql.types import *
from google.cloud import storage
import json
import time
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .master('yarn') \
        .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
        .appName("Project1") \
        .getOrCreate()

#Funcion para leer tabla desde postgresql to DataFrame pySpark
def read_csv(sheetName1,sheetName2):
    #conexión mediante jbdc a postgresql
    dfUserExtra = spark.read.format("jdbc").option("url", "jdbc:postgresql://35.237.48.113:5432/huntydatabase") \
        .option("driver", "org.postgresql.Driver").option("dbtable", sheetName1) \
        .option("user", "postgres").option("password", "dch1234").load()
    
    dfMainUser = spark.read.format("jdbc").option("url", "jdbc:postgresql://35.237.48.113:5432/huntydatabase") \
        .option("driver", "org.postgresql.Driver").option("dbtable", sheetName2) \
        .option("user", "postgres").option("password", "dch1234").load()
    
    return dfUserExtra, dfMainUser

#Funcion que toma un string, lo transforma en json y extrae el tamño de un campo lista.
#Si el tamño de la lista el valor se conserva, si no, se genera un flag.
def condition_json(x):
    valor = x.replace("\n","").replace("   ","").replace("  ","")
    valueJson= json.loads(valor)
    try:
        
        num = len(valueJson['location_change_city_ids']['list'])
        if num > 1 :
            result = valor
        else :
            result = "flag_delete"
    except:
        result = valor
    
    return result

#Funcion para realizar split del diccionario presente en un string.
#El string contiene un valor de año y mes, se extrae cada uno de estos valores
def new_columns(datos_news):
    valor = datos_news.replace("\n","").replace("   ","").replace("  ","").replace("'",'"')
    valueJson= json.loads(valor)
    
    year = valueJson['years']
    m = valueJson['months']
    
    if m == 12:
        year = year + 1 
        m = 0
        
    return year, m

#Función para la transformación del DataFrame user_extra_info.
def process_user_extra(dfUserExtra):
    #Conviertir los datos de la columna location_change_city_ids a un lista en pandas    
    #aplicar a cada campo de la lista la condición de la función condition_json
    datosLocation=dfUserExtra.select(dfUserExtra.location_change_city_ids).toPandas()['location_change_city_ids']
    new_list = datosLocation.apply(lambda x : condition_json(x))

    #convertir los datos de la columna years_experience a una lista en pandas.
    #Almacenar los valores de year & month en una nueva lista independiente
    datos_years=dfUserExtra.select(dfUserExtra.years_experience).toPandas()['years_experience']
    yearList = []
    monthList = []
    for i in range(len(datos_years)):
        year, m = new_columns(datos_years[i])
        yearList.append(year)
        monthList.append(m)
    
    #   Procesamiento DataFrame en PySpark 
    #1 :Nueva columna new_flag generada por la condicion de location_change_city_ids
    #2: P3--> Nueva columna years a partir de la lista yearList
    #3: P3--> Nueva columna month a partir de la lista monthList
    #4: Eliminar columna years_experience
    #5: Eliminar columna location_change_city_ids
    #6: P1--> Eliminar filas que tienen menos de 1 item
    #7: P2--> Eliminar filas vacancy_area_id<2 EXCLUYENDO donde employment_status =0
    dfUserExtra = dfUserExtra.repartition(1).withColumn(
        "new_flag", 
        udf(lambda id: new_list[id])(monotonically_increasing_id())) \
        .repartition(1).withColumn(
        "years", 
        udf(lambda id: yearList[id])(monotonically_increasing_id())) \
        .repartition(1).withColumn(
        "month", 
        udf(lambda id: monthList[id])(monotonically_increasing_id())) \
        .drop("years_experience") \
        .drop("location_change_city_ids") \
        .withColumnRenamed("new_flag", "location_change_city_ids") \
        .filter(col("location_change_city_ids") != "flag_delete") \
        .withColumn("vacancy_area_id", col("vacancy_area_id").astype(IntegerType())) \
        .withColumn("employment_status", col("employment_status").astype(IntegerType())) \
        .filter((col("vacancy_area_id") > 2) | (col("employment_status") == 0))
    
    
    return dfUserExtra

#Función para la transformación del DataFrame main_user_info.
def process_main_user(dfMainUser):
    
    #1 : load_date a formato fecha
    #2: Conservar solo primera letra mayuscula en last_name
    dfMainUser2= dfMainUser.withColumn("load_date", col("load_date").astype(DateType())) \
            .withColumn("last_name", initcap(col('last_name')))
    
    return dfMainUser2

#Función para escribir en BQ
def write_BQ(bq_dataset, tableName, gcs_bucket, df):
    start = time.time()    
    df.write \
      .format("bigquery") \
      .option("table","{}.{}".format(bq_dataset, tableName)) \
      .option("temporaryGcsBucket", gcs_bucket) \
      .mode('overwrite') \
      .save()

    end = time.time()
    print("The time of execution of above program is :", end-start)

if __name__ == "__main__":
    #definicion variables
    sheetName1 = "user_extra_info"
    sheetName2 = "main_user_info"
    gcs_bucket= "bucket-hunty/python"
    bq_dataset = "huntyTest"
    tableName = "a_un_paso_de_trabajar_en_hunty"

    #leer tabla from postgresql
    dfUserExtra, dfMainUser = read_csv(sheetName1,sheetName2)

    #procesamiento tabla user_extra_info
    dfUserExtra = process_user_extra(dfUserExtra)

    #procesamiento table main_user_info
    dfMainUser = process_main_user(dfMainUser)

    #Join DataFrames resultantes por columna user_id eliminando duplicados
    dfMainUser = dfMainUser.withColumnRenamed('user_id', 'user_id2')
    dfFInal= dfMainUser.join(dfUserExtra,dfMainUser.user_id2 ==  dfUserExtra.user_id,"inner").drop("user_id2")

    #Escritura en BQ
    write_BQ(bq_dataset, tableName, gcs_bucket,dfFInal)