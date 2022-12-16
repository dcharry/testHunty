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
        .appName("Project1") \
        .getOrCreate()

#Funcion para leer Data desde google Sheet a DataFrame pySpark
def read_dataframes(sheetId,sheetName1,sheetName2):
    #leer como pandas Dataframe la primera tabla
    url1 = f'https://docs.google.com/spreadsheets/d/{sheetId}/gviz/tq?tqx=out:csv&sheet={sheetName1}'
    df1 =pd.read_csv(url1)
    
    #Leer como pandas Dataframe la segunda tabla
    url2 = f'https://docs.google.com/spreadsheets/d/{sheetId}/gviz/tq?tqx=out:csv&sheet={sheetName2}'
    df2 =pd.read_csv(url2)
    
    #Limpieza columnas Unname creadas en df1
    df1.drop(df1.filter(regex="Unname"),axis=1, inplace=True)
    df1.columns
    
    #Definición Schema tabla N°1
    schema1 = StructType([StructField("user_id", StringType(), True)\
                   ,StructField("vacancy_area_id", StringType(), True)\
                     ,StructField("location_change_city_ids", StringType(), True)\
                     ,StructField("available_time_week_id", StringType(), True)\
                      ,StructField("vacancy_area_custom", StringType(), True)\
                      ,StructField("change_city", StringType(), True)\
                      ,StructField("years_experience", StringType(), True)\
                   ,StructField("employment_status", StringType(), True)])
    #pandas to pyspark --> creacion pySpark Dataframe tabla N°1
    dfUserExtra =spark.createDataFrame(df1, schema=schema1)
    
    #Definición Schema tabla N°2
    schema2 = StructType([StructField("user_id", StringType(), True)\
                   ,StructField("first_name", StringType(), True)\
                     ,StructField("last_name", StringType(), True)\
                     ,StructField("Phone", StringType(), True)\
                   ,StructField("load_date", StringType(), True)])
    #pandas to pyspark --> creacion pySpark Dataframe tabla N°2
    dfMainUser =spark.createDataFrame(df2, schema=schema2)
    
    return dfUserExtra, dfMainUser

#Funcion para el guardado de pySpark DataFrames en postgresql de GCP
def write_dataframes(dfUserExtra,dfMainUser,sheetName1,sheetName2):
    
    #Guardo Dataframes en postgresql gcp en el dataset previamente creado huntydatabase mediante 
    #jdbc y pySpark.
    #Requiere como parametros el nombre de la tabla, dataset, ip publica SQL, credenciales y driver
    dfUserExtra.select("user_id","vacancy_area_id","location_change_city_ids","available_time_week_id", "vacancy_area_custom", "change_city", "years_experience", "employment_status").write.format("jdbc")\
        .option("url", "jdbc:postgresql://35.237.48.113:5432/huntydatabase") \
        .option("driver", "org.postgresql.Driver").option("dbtable", sheetName1) \
        .option("user", "postgres").option("password", "dch1234").save()
    
    dfMainUser.select("user_id","first_name","last_name","Phone","load_date").write.format("jdbc")\
        .option("url", "jdbc:postgresql://35.237.48.113:5432/huntydatabase") \
        .option("driver", "org.postgresql.Driver").option("dbtable", sheetName2) \
        .option("user", "postgres").option("password", "dch1234").save()




if __name__ == "__main__":
    #lectura variables de entrada
    sheetId = "1yGPAiAhRz36LrVHXg3MHB7UkIjPtDDiZFJUgvP5_tY0"
    sheetName1 = "user_extra_info"
    sheetName2 = "main_user_info"

    #llamado funcion leer Data from google sheet to pySpark
    read_dataframes(sheetId,sheetName1,sheetName2)

    #llamado funcion escribir from pySpark to postgresql 
    write_dataframes(dfUserExtra,dfMainUser,sheetName1,sheetName2)