import pandas as pd
from google.cloud import storage
import gspread
import gspread_dataframe as gd

# Funcion para la escritura de DataFrames en google Sheet, cada DataFrame escrito
# corresponde a una sheet
def write_sheet(spreadSheet, worksheetName, df):
    #Conexion mediante credenciales al servicio de google sheet
    gc = gspread.service_account()

    #cargo speadSheet
    sp = gc.open(spreadSheet)

    #creo worksheet
    worksheet = sp.add_worksheet(title=worksheetName, rows="200", cols="100")

    #Guardo DataFrame en worksheet
    worksheet.clear()
    gd.set_with_dataframe(worksheet=worksheet,dataframe=df,include_index=False,include_column_header=True,resize=True)

def read_write_json(bucketName,  prefix ,spreadSheet, delimiter = None):
    #inicializo cliente storage
    storage_client = storage.Client()

    blobs = storage_client.list_blobs(bucketName, prefix=prefix, delimiter=delimiter)

    #obtener cada archivo .json a trabajar
    for blob in blobs:
        if ".json" in blob.name:
            print(blob.name)

            #leer cada json en el bucket como un DataFrame
            path = "gs://"+ bucketName +"/" + blob.name
            df = pd.read_json(path)

            #obtener el nombre del archivo como worksheetName
            file = blob.name.split('/')[1]
            worksheetName= file.split('.')[0]

            write_sheet(spreadSheet, worksheetName, df)

if __name__ == "__main__":
    #variables
    bucketName= "test-technical"
    prefix = "filesJson/"
    spreadSheet= "testHunty"

    #llamado funcion main
    read_write_json(bucketName,  prefix, spreadSheet)


