import logging
from azure.storage.blob import BlobServiceClient
import azure.functions as func
from sklearn.neighbors import KNeighborsClassifier
import pickle
import pandas as pd
import json


def read_model_from_datalake(blobname):
    STORAGEACCOUNTURL = "https://adlszeus.blob.core.windows.net"
    STORAGEACCOUNTKEY = "ksL9a2OZFCiKFYPn6hzTNJcY4WI2Nq2xSsRlUD8cDH3dBBEvePAhJqErSP6QKN27so/2ayW3DnO7O8s4uPtUZA=="
    CONTAINERNAME = "pangea-velocity-app"
    # BLOBNAME = "AzureFunctionLogs/temp1.csv"
    BLOBNAME = blobname

    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)

    # blob_client_instance.upload_blob(data=df.to_json(orient='records'))
    in_file = blob_client_instance.download_blob()

    return pd.read_json(in_file, orient="records") # type: ignore



def read_picklefile_from_datalake(blobname):
    STORAGEACCOUNTURL = "https://adlszeus.blob.core.windows.net"
    STORAGEACCOUNTKEY = "ksL9a2OZFCiKFYPn6hzTNJcY4WI2Nq2xSsRlUD8cDH3dBBEvePAhJqErSP6QKN27so/2ayW3DnO7O8s4uPtUZA=="
    CONTAINERNAME = "pangea-velocity-app"
    # BLOBNAME = "AzureFunctionLogs/temp1.csv"
    BLOBNAME = blobname

    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)

    # blob_client_instance.upload_blob(data=df.to_json(orient='records'))

    in_file = blob_client_instance.download_blob(0)
    stream = in_file.readall()

    return pickle.loads(stream) # type: ignore



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        model = read_picklefile_from_datalake('Models/Clustering/all_cluster_classification_model.pkl')
        scalar = read_picklefile_from_datalake('Scalars/Clustering/all_cluster_classification_scalar.pkl')
        in_df = pd.read_json(req.files['file'])

        prediction = model.predict(pd.DataFrame(scalar.transform(in_df[['GR', 'NPHI', 'RHOB', 'RSHALLOW']]), columns=['GR', 'NPHI', 'RHOB', 'RSHALLOW']))

        in_df['Cluster_No'] = prediction

        return func.HttpResponse(
             json.dumps(in_df.to_json()),
             status_code=200
        )
    except:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200)
    
