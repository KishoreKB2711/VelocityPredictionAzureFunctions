import json
import logging

import azure.functions as func


from azure.storage.blob import BlobServiceClient
from sklearn.neighbors import KNeighborsClassifier
import pickle
import pandas as pd


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

    return pd.read_json(in_file) # type: ignore


def write_file_to_datalake(blobname, df):
    STORAGEACCOUNTURL = "https://adlszeus.blob.core.windows.net"
    STORAGEACCOUNTKEY = "ksL9a2OZFCiKFYPn6hzTNJcY4WI2Nq2xSsRlUD8cDH3dBBEvePAhJqErSP6QKN27so/2ayW3DnO7O8s4uPtUZA=="
    CONTAINERNAME = "pangea-velocity-app"
    # BLOBNAME = "AzureFunctionLogs/temp1.csv"
    BLOBNAME = blobname

    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)

    # blob_client_instance.upload_blob(data=df.to_json(orient='records'))
    blob_client_instance.upload_blob(data=df.to_json(), overwrite=True)

    return True



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

    return pickle.loads(stream)


def main(event: func.EventGridEvent):
    
    model = read_picklefile_from_datalake('Models/Clustering/all_cluster_classification_model.pkl')
    scalar = read_picklefile_from_datalake('Scalars/Clustering/all_cluster_classification_scalar.pkl')

    in_df = read_model_from_datalake(event.get_json()['BLOBNAME'])

    prediction = model.predict(pd.DataFrame(scalar.transform(in_df[['GR', 'NPHI', 'RHOB', 'RSHALLOW']]), columns=['GR', 'NPHI', 'RHOB', 'RSHALLOW']))

    in_df['Cluster_No'] = prediction

    # write_file_to_datalake(f"Results/{event.get_json()['Company']}_{event.get_json()['Well_Name']}_{event.get_json()['API']}.json", in_df)
    write_file_to_datalake(f"Results/clustering_result.json", in_df)