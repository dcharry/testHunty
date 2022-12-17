# DATA ENGINEER TEST HUNTY

## Didier F. Charry N - Data Engineer

### ESTRUCTURA DEL PROYECTO

*El proyecto contiene 2 carpetas:*
#### uploadSheet 
*Esta carpeta contiene el .py para tomar un json desde un bucket en gcp y cargarlo a google sheet*
#### ETL
*contiene dos .py en su interior:*
##### uploadToPostgresql.py
*Script que toma 2 tablas desde google sheet y las expone en un dataset en postgresql de GCP*
##### etl.py
*ETL que toma dos tablas desde postgresql de GCP, las transforma y las expone en un dataset de GCP*


### CONCEPTOS IMPORTANTES - COMANDOS GCP

#### Como crear una instacia postgresql con su respectivo dataset:
gcloud sql instances create mypostgre --database-version=POSTGRES_14 --cpu=2 --memory=7680MB --region=us-east1
gcloud sql users set-password postgres --instance=mypostgre --password=dch1234
gcloud sql connect mypostgre --user=postgres

CREATE DATABASE huntydatabase;

#### Como crear una instancia en DATAPROC:
gcloud dataproc clusters create hunty-demo \
--region us-east1 \
--scopes sql-admin \
--single-node \
--master-machine-type n1-standard-8 \
--master-boot-disk-size 500 \
--image-version 2.0-debian10 \
--initialization-actions gs://goog-dataproc-initialization-actions-us-east1/cloud-sql-proxy/cloud-sql-proxy.sh \
--metadata "enable-cloud-sql-hive-metastore=false" \
--metadata "additional-cloud-sql-instances=postgrespoc-371714:us-east1:mypostgre" 

#### Como crear un JOB PySpark:
gcloud dataproc jobs submit pyspark uploadToPostgresql.py --cluster=hunty-demo --region=us-east1 --jars=gs://bucket-hunty/jars/postgresql-42.2.5.jar

### RESUMEN ARQUITECTURA

##### uploadToPostgresql.py:
*1. Se crea instancia en SQL.*

*2. Build sparkSession.*

*3. Se construye script para leer datos desde Google Sheet y escribirlos en postgresql.*

*4. Se crea VM en DATAPRO.C*

*5 se construye el Job.*

*6. Ejecución.*

##### etl.py:
*1. Build sparkSession.*

*2. Se construye script para leer datos desde postgresql, transformarlos y escribirlos en BQ.*

*3. Se crea VM en DATAPROC.*

*4 se construye el Job.*

*5. Ejecución.*


### RESULTADO FINAL

#### GCP to GOOGLE SHEET
![image](https://user-images.githubusercontent.com/40002518/208236340-4e367322-eaef-40f5-bd9f-da1ce3a4d783.png)

#### ETL
![image](https://user-images.githubusercontent.com/40002518/208236292-8a36a92e-92c6-4ff3-9f88-197a9425aa28.png)
