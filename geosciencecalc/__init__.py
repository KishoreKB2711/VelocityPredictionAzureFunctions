import logging
import math
import json 

import azure.functions as func
from azure.storage.blob import BlobServiceClient

import pandas as pd

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


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    geo_config={}
    name = req.params.get('name')
    well_name = req.form['well_name']
    model_name = req.form['model_name']

    input_df = read_dataframe_from_datalake(blobname=f"Results/{well_name}/{well_name}_{model_name}.json")

    if req.form['model'] == 'pangea':
        geo_config = read_dataframe_from_datalake('Config_files/geo_config.json').to_dict()
    elif req.form['model'] == 'usersingle':
        geo_config = json.loads(req.form['config'])
    else:
        geo_config = read_dataframe_from_datalake('Config_files/geo_config.json').to_dict()


    ######################################################################################################
    # Variable Inputs
    ######################################################################################################
    input_df['Vp_45'] = input_df.apply(lambda x: (geo_config['Vp45']['M']) * x[f'VP_{model_name}'] + geo_config['Vp45']['C'], axis=1)
    input_df['Vp_90'] = input_df.apply(lambda x: (geo_config['Vp90']['M']) * x[f'VP_{model_name}'] + geo_config['Vp90']['C'], axis=1)
    input_df['Vs_90'] = input_df.apply(lambda x: (geo_config['Vs90']['M']) * x[f'VS_{model_name}'] + geo_config['Vs90']['C'], axis=1)

    ######################################################################################################

    input_df['Vp_45_GPa'] = input_df.apply(lambda x: (((x['RHOB'] * x['Vp_45'] * x['Vp_45'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894 , axis=1)
    input_df['Vp_45_Sq_GPa'] = input_df.apply(lambda x: x['Vp_45_GPa'] * x['Vp_45_GPa'] , axis=1)

    input_df['S11'] = input_df.apply(lambda x:(((x['RHOB'] * x['Vp_90'] * x['Vp_90'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894, axis=1) 
    input_df['S33'] = input_df.apply(lambda x:(((x['RHOB'] * x[f'VP_{model_name}'] * x[f'VP_{model_name}'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894, axis=1) 
    input_df['S44'] = input_df.apply(lambda x:(((x['RHOB'] * x[f'VS_{model_name}'] * x[f'VS_{model_name}'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894, axis=1) 
    input_df['S66'] = input_df.apply(lambda x:(((x['RHOB'] * x['Vs_90'] * x['Vs_90'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894, axis=1) 
    
    input_df['S12'] = input_df.apply(lambda x: x['S11'] - (2 * x['S66']), axis=1) 
    input_df['S13'] = input_df.apply(lambda x: -x['S44'] + math.sqrt((4 * x['Vp_45_Sq_GPa']) - ((2 * x['Vp_45_GPa'] ) * (x['S11'] + x['S33'] + (2 * x['S44'])))+((x['S11'] + x['S44'])*(x['S33'] + x['S44']))), axis=1)
    
    input_df['Edva'] = input_df.apply(lambda x:((x['S33'] - (2 * x['S13'] * x['S13']) / (x['S11'] + x['S12'])) * 145037.73773) / 1000000, axis=1)
    input_df['Edha'] = input_df.apply(lambda x:((x['S11'] - x['S12']) * (x['S11'] * x['S33'] - (2 * x['S13'] * x['S13']) + (x['S12'] * x['S33']))/((x['S11'] * x['S33']) - (x['S13'] * x['S13'])) * 145037.73773) / 1000000, axis=1)

    input_df['PRdva'] = input_df.apply(lambda x: x['S13'] / (x['S11'] + x['S12']), axis=1)
    input_df['PRdha'] = input_df.apply(lambda x:(((x['S33'] * x['S12']) - (x['S13'] * x['S13'])) / ((x['S33'] * x['S11']) - (x['S13'] * x['S13']))), axis=1)

    ######################################################################################################
    # Variable Inputs
    ######################################################################################################
    input_df['Esv'] = input_df.apply(lambda x:(geo_config['Esv']['M'] * x['Edva']) - geo_config['Esv']['C'], axis=1)
    input_df['Esh'] = input_df.apply(lambda x:(geo_config['Esh']['M'] * x['Edha']) - geo_config['Esh']['C'], axis=1)

    input_df['PRsv'] = input_df.apply(lambda x:(geo_config['PRsv']['M'] * x['PRdva']) + geo_config['PRsv']['C'], axis=1)
    input_df['PRsh'] = input_df.apply(lambda x:(geo_config['PRsh']['M'] * x['PRdha']) - geo_config['PRsh']['C'], axis=1)
    ######################################################################################################

    input_df['Esv_GPa'] = input_df.apply(lambda x: x['Esv'] / 0.147, axis=1)
    input_df['Esh_GPa'] = input_df.apply(lambda x: x['Esh'] / 0.147, axis=1)

    input_df['C11'] = input_df.apply(lambda x: (((1-((x['Esh_GPa'] / x['Esv_GPa']) * x['PRsv'] * x['PRsv']))/(x['Esv_GPa'] * x['Esh_GPa'] * (((1 + x['PRsh']) * (1 - x['PRsh'] - (2 * (x['Esh_GPa'] / x['Esv_GPa']) * x['PRsv'] * x['PRsv']))) / (x['Esv_GPa'] * x['Esh_GPa'] * x['Esh_GPa']))))), axis=1)
    input_df['C12'] = input_df.apply(lambda x: (x['PRsh'] + ((x['Esh_GPa'] / x['Esv_GPa']) * x['PRsv'] * x['PRsv'])) / ((((1 + x['PRsh']) * (1 - x['PRsh'] - (2 * (x['Esh_GPa'] / x['Esv_GPa']) * x['PRsv'] * x['PRsv']))) / (x['Esv_GPa'] * x['Esh_GPa'] * x['Esh_GPa'])) * x['Esv_GPa'] * x['Esh_GPa']), axis=1)
    input_df['C13'] = input_df.apply(lambda x: (x['PRsv'] * (1 + x['PRsh'])) / (x['Esv_GPa'] * x['Esh_GPa'] * (((1 + x['PRsh']) * (1 - x['PRsh'] - (2 * (x['Esh_GPa'] / x['Esv_GPa']) * x['PRsv'] * x['PRsv'])))/(x['Esv_GPa'] * x['Esh_GPa'] * x['Esh_GPa']))), axis=1)
    input_df['C33'] = input_df.apply(lambda x: (1 - (x['PRsh'] * x['PRsh'])) / ((((1 + x['PRsh']) * (1 - x['PRsh'] - (2 * (x['Esh_GPa'] / x['Esv_GPa']) * x['PRsv'] * x['PRsv'])))/(x['Esv_GPa'] * x['Esh_GPa'] * x['Esh_GPa'])) * x['Esh_GPa'] * x['Esh_GPa']), axis=1)
    
    #### Ask Santosh about NPHI
    input_df['Ad'] = input_df.apply(lambda x: x['RHOB'] / (1 - (x['NPHI'] / 1)), axis=1)    
    input_df['As'] = input_df.apply(lambda x: ((x['S44'] / x['S33']) + (x['S66'] / x['S11'])) / 2 if x[f'VP_{model_name}']>23000 else (1 / (1 - x['PRdva']))*((x['S44'] / x['S33']) + (x['S66'] / x['S11'])) / 2, axis=1)
    
    input_df['V_Biot'] = input_df.apply(lambda x: 1 - ((x['As']) * ((((2 * x['C13']) + x['C33']) / (3 * (((((((x['Ad'] * ((x[f'VP_{model_name}'] * 12 * 2.54 * 12 * 2.54 * x[f'VP_{model_name}']) - (4 * x[f'VS_{model_name}'] * 12 * 2.54 * 12 * 2.54 * x[f'VS_{model_name}'] / 3)))) / 1000000) / 68900) * 6.894757) + (((((x['Ad'] * ((x['Vp_90'] * 12 * 2.54 * 12 * 2.54 * x['Vp_90']) - (4 * x['Vs_90'] * 12 * 2.54 * 12 * 2.54 * x['Vs_90'] / 3)))) / 1000000) / 68900) * 6.894757)) / 2))))), axis=1)
    input_df['H_Biot'] = input_df.apply(lambda x: 1 - (x['As'] * ((x['C11'] + x['C12'] + x['C13']) / (3 * (((((((x['Ad'] * ((x[f'VP_{model_name}'] * 12 * 2.54 * 12 * 2.54 * x[f'VP_{model_name}']) - (4 * x[f'VS_{model_name}'] * 12 * 2.54 * 12 * 2.54 * x[f'VS_{model_name}'] / 3)))) / 1000000) / 68900) * 6.894757) + (((((x['Ad'] * ((x['Vp_90'] * 12 * 2.54 * 12 * 2.54 * x['Vp_90']) - (4 * x['Vs_90'] * 12 * 2.54 * 12 * 2.54 * x['Vs_90'] / 3)))) / 1000000) / 68900) * 6.894757)) / 2)))), axis=1)
 
    write_file_to_datalake(f'Geo_Results/{well_name}/{well_name}_{model_name}.json', input_df)
    return func.HttpResponse(f"Success")

    # if not name:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         name = req_body.get('name')

    # if name:
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )
