# DATA ENGINEER TEST HUNTY

## Didier F. Charry N - Data Engineer

### ESTRUCTURA DEL PROYECTO

*El proyecto contiene 2 carpetas:*
#### uploadSheet 
*Esta carpeta contiene el .py para tomar un json desde un bucket en gcp y cargarlo a google sheet*
#### ETL
*contiene dos .py en su interior:*
##### uploadToPostgresql.py
*Script que toma 2 tablas desde google sheet y las expone en un dataset en postgresql de GCP
##### etl.py
*ETL que toma dos tablas desde postgresql de GCP, las transforma y las expone en un dataset de GCP
