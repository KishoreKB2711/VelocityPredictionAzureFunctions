import json
import logging

import azure.functions as func

from azure.storage.blob import BlobServiceClient
import pickle
import pandas as pd

import sklearn
import tensorflow as tf
from tensorflow import keras
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans
from sklearn.neighbors import KNeighborsClassifier

def List_blob_contents(name_start_with=None):
    STORAGEACCOUNTURL = "https://adlszeus.blob.core.windows.net"
    STORAGEACCOUNTKEY = "ksL9a2OZFCiKFYPn6hzTNJcY4WI2Nq2xSsRlUD8cDH3dBBEvePAhJqErSP6QKN27so/2ayW3DnO7O8s4uPtUZA=="
    CONTAINERNAME = "pangea-velocity-app"

    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

    container_client = blob_service_client_instance.get_container_client(CONTAINERNAME)
    blob_list = list(container_client.list_blobs(name_starts_with=name_start_with))

    return blob_list

def read_json_from_datalake(blobname):
    STORAGEACCOUNTURL = "https://adlszeus.blob.core.windows.net"
    STORAGEACCOUNTKEY = "ksL9a2OZFCiKFYPn6hzTNJcY4WI2Nq2xSsRlUD8cDH3dBBEvePAhJqErSP6QKN27so/2ayW3DnO7O8s4uPtUZA=="
    CONTAINERNAME = "pangea-velocity-app"
    # BLOBNAME = "AzureFunctionLogs/temp1.csv"
    BLOBNAME = blobname

    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)

    # blob_client_instance.upload_blob(data=df.to_json(orient='records'))
    in_file = blob_client_instance.download_blob()

    return json.load(in_file) # type: ignore


def read_dataframe_from_datalake(blobname):
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

    blob_client_instance.close()

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
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    model_list = List_blob_contents('Models/Test Model')
    scalar_list = List_blob_contents('Scalars/Test_Model')
    classification_list = List_blob_contents('Models/Classification')

    model = {}
    scalar = {}
    scalar_input_config = read_json_from_datalake("Config_files/scalar_input_config.json")

    for model_name in model_list:
        if '.pkl' in model_name.name:
            model[model_name.name.split('/')[-1].replace(".pkl", "")] = read_picklefile_from_datalake(model_name.name)
    
    for scalar_name in scalar_list:
        if '.pkl' in scalar_name.name:
            scalar[scalar_name.name.split('/')[-1].replace(".pkl", "")] = read_picklefile_from_datalake(scalar_name.name)

    for cl_model_name in classification_list:
        if '.pkl' in cl_model_name.name:
            model[cl_model_name.name.split('/')[-1].replace(".pkl", "")] = read_picklefile_from_datalake(cl_model_name.name)

    # model = read_picklefile_from_datalake('Models/Clustering/all_cluster_classification_model.pkl')
    # scalar = read_picklefile_from_datalake('Scalars/Clustering/all_cluster_classification_scalar.pkl')

    in_df = read_dataframe_from_datalake(event.get_json()['BLOBNAME'])
    test_final_df_complete = in_df.drop(columns=['DEPTH'])

    # prediction = model.predict(pd.DataFrame(scalar.transform(in_df[['GR', 'NPHI', 'RHOB', 'RSHALLOW']]), columns=['GR', 'NPHI', 'RHOB', 'RSHALLOW']))
    # in_df['Cluster_No'] = prediction



    ### Test
    #### Importing Test Data
    # test_final_df = pd.read_json("Data/tcom_final_test_data.json")
    # test_final_df.to_csv("Data/tcom_final_test_data_records.csv", index=False)
    # test_final_df_Vp = test_final_df.drop(columns=['RDEEP', 'VS'])
    # test_final_df_Vs = test_final_df.drop(columns=['RDEEP', 'VP'])
    # test_final_df_complete = test_final_df.drop(columns=['RDEEP', 'VP', 'VS'])


    #### Getting ClusterDetails
    scaled_cluster_df = pd.DataFrame(scalar['Classifcation_scaler'].transform(test_final_df_complete[['GR', 'NPHI', 'RHOB', 'RSHALLOW']]), columns=['GR', 'NPHI', 'RHOB', 'RSHALLOW'])
    test_final_df_complete['GR_VP_Cluster'] = model['gr_vp_cluster_classification_model'].predict(scaled_cluster_df)
    test_final_df_complete['NPHI_VP_Cluster'] = model['nphi_vp_cluster_classification_model'].predict(scaled_cluster_df)
    test_final_df_complete['RHOB_VP_Cluster'] = model['rhob_vp_cluster_classification_model'].predict(scaled_cluster_df)
    test_final_df_complete['RSHALLOW_VP_Cluster'] = model['rshallow_vp_cluster_classification_model'].predict(scaled_cluster_df)
    test_final_df_complete['GR_VS_Cluster'] = model['gr_vs_cluster_classification_model'].predict(scaled_cluster_df)
    test_final_df_complete['NPHI_VS_Cluster'] = model['nphi_vs_cluster_classification_model'].predict(scaled_cluster_df)
    test_final_df_complete['RHOB_VS_Cluster'] = model['rhob_vs_cluster_classification_model'].predict(scaled_cluster_df)
    test_final_df_complete['RSHALLOW_VS_Cluster'] = model['rshallow_vs_cluster_classification_model'].predict(scaled_cluster_df)
    # test_final_df_Vp['GR_VP_Cluster'] = gr_vp_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vp['NPHI_VP_Cluster'] = nphi_vp_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vp['RHOB_VP_Cluster'] = rhob_vp_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vp['RSHALLOW_VP_Cluster'] = rshallow_vp_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vp['GR_VS_Cluster'] = gr_vs_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vp['NPHI_VS_Cluster'] = nphi_vs_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vp['RHOB_VS_Cluster'] = rhob_vs_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vp['RSHALLOW_VS_Cluster'] = rshallow_vs_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vs['GR_VP_Cluster'] = gr_vp_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vs['NPHI_VP_Cluster'] = nphi_vp_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vs['RHOB_VP_Cluster'] = rhob_vp_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vs['RSHALLOW_VP_Cluster'] = rshallow_vp_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vs['GR_VS_Cluster'] = gr_vs_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vs['NPHI_VS_Cluster'] = nphi_vs_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vs['RHOB_VS_Cluster'] = rhob_vs_cluster_classification_model.predict(scaled_cluster_df)
    # test_final_df_Vs['RSHALLOW_VS_Cluster'] = rshallow_vs_cluster_classification_model.predict(scaled_cluster_df)
    # #### Predicting Vp for individual cluster
    # prediction = {}

    clusters = test_final_df_complete.filter(like='_Cluster').columns.tolist()

    for cluster in clusters:
        groups = test_final_df_complete.groupby(cluster)   

        for name, group in groups:
            test_final_df_complete[f'{cluster}_{name}_VP'] = model['models_vp'][cluster][name-1].predict(pd.DataFrame(scalar['scaler_Vp'][cluster][name-1].transform(test_final_df_complete[['GR', 'NPHI', 'RHOB', 'RSHALLOW']]), columns=['GR', 'NPHI', 'RHOB', 'RSHALLOW']))
    #### Predicting Vs for individual cluster
    prediction = {}

    for cluster in clusters:
        groups = test_final_df_complete.groupby(cluster)   
        
        for name, group in groups:
            test_final_df_complete[f'{cluster}_{name}_VS'] = model['models_vs'][cluster][name-1].predict(pd.DataFrame(scalar['scaler_Vs'][cluster][name-1].transform(test_final_df_complete[['GR', 'NPHI', 'RHOB', 'RSHALLOW']]), columns=['GR', 'NPHI', 'RHOB', 'RSHALLOW']))
    
    #### Complete
    # test_final_df_complete['NPHI_VP_Cluster_4_VP'] = test_final_df_complete.filter(like='VP').drop(columns=['GR_VP_Cluster', 'NPHI_VP_Cluster', 'RHOB_VP_Cluster', 'RSHALLOW_VP_Cluster']).mean(axis=1)
    # test_final_df_complete['NPHI_VS_Cluster_7_VP'] = test_final_df_complete.filter(like='VS').drop(columns=['GR_VS_Cluster', 'NPHI_VS_Cluster', 'RHOB_VS_Cluster', 'RSHALLOW_VS_Cluster']).mean(axis=1)
    # test_final_df_complete['NPHI_VP_Cluster_4_VS'] = test_final_df_complete.filter(like='VP').drop(columns=['GR_VP_Cluster', 'NPHI_VP_Cluster', 'RHOB_VP_Cluster', 'RSHALLOW_VP_Cluster']).mean(axis=1)
    # test_final_df_complete['NPHI_VS_Cluster_7_VS'] = test_final_df_complete.filter(like='VS').drop(columns=['GR_VS_Cluster', 'NPHI_VS_Cluster', 'RHOB_VS_Cluster', 'RSHALLOW_VS_Cluster']).mean(axis=1)
    # test_final_df_complete = test_final_df_complete.drop(columns=['DEPTH'])
    # test_final_df_complete.head()
    
    final_vp_complete_prediction = model['final_Vp_complete_model'].predict(pd.DataFrame(scalar['final_Vp_complete_scaler'].transform(test_final_df_complete[scalar_input_config['final_Vp_complete_scaler']]), columns=scalar_input_config['final_Vp_complete_scaler']))
    final_vs_complete_prediction = model['final_Vs_complete_model'].predict(pd.DataFrame(scalar['final_Vs_complete_scaler'].transform(test_final_df_complete[scalar_input_config['final_Vs_complete_scaler']]), columns=scalar_input_config['final_Vs_complete_scaler']))
    in_df['VP_complete_Predicted'] = final_vp_complete_prediction
    in_df['VS_complete_Predicted'] = final_vs_complete_prediction

    clustering_model = read_picklefile_from_datalake('Models/Clustering/all_cluster_classification_model.pkl')
    clustering_scalar = read_picklefile_from_datalake('Scalars/Clustering/all_cluster_classification_scalar.pkl')

    prediction = clustering_model.predict(pd.DataFrame(clustering_scalar.transform(in_df[['GR', 'NPHI', 'RHOB', 'RSHALLOW']]), columns=['GR', 'NPHI', 'RHOB', 'RSHALLOW']))

    in_df['Cluster_No'] = prediction

    write_file_to_datalake(f"Results/prediction_result.json", in_df)

    # write_file_to_datalake(f"Results/clustering_result.json", in_df)
    logging.info('Python EventGrid trigger processed an event: %s', result)
