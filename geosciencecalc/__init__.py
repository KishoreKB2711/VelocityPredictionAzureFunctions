import logging
import math
import json 

import azure.functions as func
from azure.storage.blob import BlobServiceClient

import pandas as pd
import numpy as np

########################
#Read from ADLS
#######################
def read_dataframe_from_datalake(blobname, flag = True):
    STORAGEACCOUNTURL = "https://adlszeus.blob.core.windows.net"
    STORAGEACCOUNTKEY = "ksL9a2OZFCiKFYPn6hzTNJcY4WI2Nq2xSsRlUD8cDH3dBBEvePAhJqErSP6QKN27so/2ayW3DnO7O8s4uPtUZA=="
    CONTAINERNAME = "pangea-velocity-app"
    # BLOBNAME = "AzureFunctionLogs/temp1.csv"
    BLOBNAME = blobname

    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)

    # blob_client_instance.upload_blob(data=df.to_json(orient='records'))
    in_file = blob_client_instance.download_blob()

    if flag:
        return pd.read_json(in_file) # type: ignore
    else:
        return json.loads(in_file._current_content)
    
########################
#Write to ADLS
#######################
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

###################################
#Data Prep and borehole correction
##################################
def dataprep(in_df, borehole_bool, min_vp_vs, max_vp_vs, caliper_bitsize):

    if (min_vp_vs == None or max_vp_vs == None or min_vp_vs >= max_vp_vs) and caliper_bitsize == None:
        return in_df
    
    elif caliper_bitsize == None:
        in_df['vp_vs'] =  in_df.apply(lambda row: row['VP'] / row['VS'], axis=1)
        in_df['vp_vs_bool'] =  in_df.apply(lambda row: True if row['vp_vs'] > min_vp_vs and row['vp_vs'] < max_vp_vs else False, axis=1)
        in_df['final_bool'] =  in_df.apply(lambda row: row['vp_vs_bool'], axis=1)

    elif min_vp_vs == None or max_vp_vs == None or min_vp_vs >= max_vp_vs:
        in_df['caliper_bitsize_bool'] =  in_df.apply(lambda row: True if row['Caliper'] > ((1 - caliper_bitsize) * row['Bitsize']) and row['Caliper'] < ((1 + caliper_bitsize) * row['Bitsize']) else False, axis=1)
        in_df['final_bool'] =  in_df.apply(lambda row: row['caliper_bitsize_bool'], axis=1)

    else:
        in_df['vp_vs'] =  in_df.apply(lambda row: row['VP'] / row['VS'], axis=1)
        in_df['vp_vs_bool'] =  in_df.apply(lambda row: True if row['vp_vs'] > min_vp_vs and row['vp_vs'] < max_vp_vs else False, axis=1)
        in_df['caliper_bitsize_bool'] =  in_df.apply(lambda row: True if row['Caliper'] > ((1 - caliper_bitsize) * row['Bitsize']) and row['Caliper'] < ((1 + caliper_bitsize) * row['Bitsize']) else False, axis=1)
        
        if borehole_bool == True:
            in_df['final_bool'] =  in_df.apply(lambda row: row['vp_vs_bool'] or row['caliper_bitsize_bool'], axis=1)
        else:
            in_df['final_bool'] =  in_df.apply(lambda row: row['vp_vs_bool'] and row['caliper_bitsize_bool'], axis=1)

    # Vp from GR: 0.3422*(GR)^2 - 124.91*(GR)^1 + 22383

    # Vs from GR: 0.1771*(GR)^2 - 45.154*(GR)^1 + 9367.8-300

    in_df['VP'] = in_df.apply(lambda row: ((0.3422 * (row['GR'] ** 2)) - (124.91 * row['GR']) + 22383) if row['final_bool'] == False else row['VP'], axis=1)
    in_df['VS'] = in_df.apply(lambda row: ((0.1771 * (row['GR'] ** 2)) - (45.154 * row['GR']) + 9367.8 - 300) if row['final_bool'] == False else row['VS'], axis=1)

    in_df =  in_df.drop(columns=['vp_vs', 'caliper_bitsize', 'vp_vs_bool', 'caliper_bitsize_bool', 'final_bool'], errors='ignore')
    
    return in_df

##############################
# Getting tops at each Depth
#############################
def assign_tops(tops_df, input_df):
    tops_depth = tops_df.filter(regex='^.*dep.*$').columns.tolist()[0]
    tops_df = tops_df.sort_values(by=tops_depth)
    tops_list = tops_df[tops_df.filter(like="Top").columns.tolist()[0]]
    tops_depth_list = tops_df[tops_depth].tolist()

    conditions = []
    i = 0
    for top in tops_list:
        try:
            conditions.append(input_df['DEPTH'].between(left=tops_depth_list[i], right=tops_depth_list[i+1], inclusive='left'))
            i+=1
        except:
            conditions.append(input_df['DEPTH'].between(left=tops_depth_list[i], right=1000000, inclusive='left'))
            i+=1
    
    return np.select(conditions, tops_list)


###########################################
# Function to calculate Dynamic properties
##########################################
def calculate_linear(tops_config, tops, col, x ):
    if tops == '0' or tops == None:
        return None
    
    return tops_config[tops]['config'][col]['M'] * x + tops_config[tops]['config'][col]['C']



def CalculateRamp(in_df, col, depth1, depth2):
    return None
 

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        project_val = req.form['project'] 
        well_val = req.form['well']
        model = req.form['model']

        
        if req.form['borehole_bool'] == '0':
            borehole_bool = False
        else:
            borehole_bool = True
        try:
            min_vp_vs = float(req.form['min_vp_vs'])
        except:
            min_vp_vs = None
        try:
            max_vp_vs = float(req.form['max_vp_vs']) 
        except:
            max_vp_vs = None
        try:
            caliper_bitsize = float(req.form['caliper_bitsize'])
        except:
            caliper_bitsize = None
            
    except:
        return func.HttpResponse(f"Missing Values", status_code=204)

    input_df = read_dataframe_from_datalake(blobname=f"Geo_Mech_Projects/Pangea/{project_val}/{well_val}.json")

    input_df = dataprep(input_df, borehole_bool, min_vp_vs, max_vp_vs, caliper_bitsize)

    geo_config={}


    #########################
    # Tops common for all types of calc
    ################
    try:
        STORAGEACCOUNTURL = "https://adlszeus.blob.core.windows.net"
        STORAGEACCOUNTKEY = "ksL9a2OZFCiKFYPn6hzTNJcY4WI2Nq2xSsRlUD8cDH3dBBEvePAhJqErSP6QKN27so/2ayW3DnO7O8s4uPtUZA=="
        CONTAINERNAME = "pangea-velocity-app"
        blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
                
        container_client = blob_service_client_instance.get_container_client(CONTAINERNAME)
        blob_list = container_client.list_blobs(name_starts_with="Tops/")
        temp = []

        for blob in blob_list:
            temp.append(blob.name)
            if well_val in blob.name:
                BLOBNAME = blob.name
                break

        blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)
        in_tops = blob_client_instance.download_blob()
        tops_df = pd.read_json(in_tops)
        tops_df = read_dataframe_from_datalake(blobname=BLOBNAME)
        tops_list = tops_df[tops_df.filter(like="Top").columns.tolist()[0]]

    except:
        return func.HttpResponse(f"Missing Tops", status_code=204)

    input_df['Tops'] = assign_tops(tops_df, input_df)






    #########################
    # Velocity Section
    ######################

    if model == 'usmulti':

        try:
            geo_config = json.loads(req.form['config'])
        except:
            return func.HttpResponse(f"Missing Values", status_code=204)

        # temp = input_df.apply(lambda x: calculate_linear(geo_config, x['Tops'], 'Vp45', x['VP']), axis=1)

        
        ######################################################################################################
        # Variable Inputs
        ######################################################################################################
        # input_df['Vp_45'] = input_df.apply(lambda x: (geo_config['Vp45']['M']) * x['VP'] + geo_config['Vp45']['C'], axis=1)
        # input_df['Vp_90'] = input_df.apply(lambda x: (geo_config['Vp90']['M']) * x['VP'] + geo_config['Vp90']['C'], axis=1)
        # input_df['Vs_90'] = input_df.apply(lambda x: (geo_config['Vs90']['M']) * x['VS'] + geo_config['Vs90']['C'], axis=1)

        input_df['Vp_45'] = input_df.apply(lambda x: calculate_linear(geo_config, x['Tops'], 'Vp45', x['VP']), axis=1)
        input_df['Vp_90'] = input_df.apply(lambda x: calculate_linear(geo_config, x['Tops'], 'Vp90', x['VP']), axis=1)
        input_df['Vs_90'] = input_df.apply(lambda x: calculate_linear(geo_config, x['Tops'], 'Vs90', x['VS']), axis=1)

        ######################################################################################################

        input_df['Vp_45_GPa'] = input_df.apply(lambda x: (((x['RHOB'] * x['Vp_45'] * x['Vp_45'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894 , axis=1)
        input_df['Vp_45_Sq_GPa'] = input_df.apply(lambda x: x['Vp_45_GPa'] * x['Vp_45_GPa'] , axis=1)

        input_df['S11'] = input_df.apply(lambda x:(((x['RHOB'] * x['Vp_90'] * x['Vp_90'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894, axis=1) 
        input_df['S33'] = input_df.apply(lambda x:(((x['RHOB'] * x['VP'] * x['VP'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894, axis=1) 
        input_df['S44'] = input_df.apply(lambda x:(((x['RHOB'] * x['VS'] * x['VS'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894, axis=1) 
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
        # input_df['Esv'] = input_df.apply(lambda x:(geo_config['Esv']['M'] * x['Edva']) - geo_config['Esv']['C'], axis=1)
        # input_df['Esh'] = input_df.apply(lambda x:(geo_config['Esh']['M'] * x['Edha']) - geo_config['Esh']['C'], axis=1)

        # input_df['PRsv'] = input_df.apply(lambda x:(geo_config['PRsv']['M'] * x['PRdva']) + geo_config['PRsv']['C'], axis=1)
        # input_df['PRsh'] = input_df.apply(lambda x:(geo_config['PRsh']['M'] * x['PRdha']) - geo_config['PRsh']['C'], axis=1)


        input_df['Esv'] = input_df.apply(lambda x: calculate_linear(geo_config, x['Tops'], 'Esv', x['Edva']), axis=1)
        input_df['Esh'] = input_df.apply(lambda x: calculate_linear(geo_config, x['Tops'], 'Esh', x['Edha']), axis=1)

        input_df['PRsv'] = input_df.apply(lambda x: calculate_linear(geo_config, x['Tops'], 'PRsv', x['PRdva']), axis=1)
        input_df['PRsh'] = input_df.apply(lambda x: calculate_linear(geo_config, x['Tops'], 'PRsh', x['PRdha']), axis=1)
        ######################################################################################################

        input_df['Esv_GPa'] = input_df.apply(lambda x: x['Esv'] / 0.147, axis=1)
        input_df['Esh_GPa'] = input_df.apply(lambda x: x['Esh'] / 0.147, axis=1)

        input_df['C11'] = input_df.apply(lambda x: (((1-((x['Esh_GPa'] / x['Esv_GPa']) * x['PRsv'] * x['PRsv']))/(x['Esv_GPa'] * x['Esh_GPa'] * (((1 + x['PRsh']) * (1 - x['PRsh'] - (2 * (x['Esh_GPa'] / x['Esv_GPa']) * x['PRsv'] * x['PRsv']))) / (x['Esv_GPa'] * x['Esh_GPa'] * x['Esh_GPa']))))), axis=1)
        input_df['C12'] = input_df.apply(lambda x: (x['PRsh'] + ((x['Esh_GPa'] / x['Esv_GPa']) * x['PRsv'] * x['PRsv'])) / ((((1 + x['PRsh']) * (1 - x['PRsh'] - (2 * (x['Esh_GPa'] / x['Esv_GPa']) * x['PRsv'] * x['PRsv']))) / (x['Esv_GPa'] * x['Esh_GPa'] * x['Esh_GPa'])) * x['Esv_GPa'] * x['Esh_GPa']), axis=1)
        input_df['C13'] = input_df.apply(lambda x: (x['PRsv'] * (1 + x['PRsh'])) / (x['Esv_GPa'] * x['Esh_GPa'] * (((1 + x['PRsh']) * (1 - x['PRsh'] - (2 * (x['Esh_GPa'] / x['Esv_GPa']) * x['PRsv'] * x['PRsv'])))/(x['Esv_GPa'] * x['Esh_GPa'] * x['Esh_GPa']))), axis=1)
        input_df['C33'] = input_df.apply(lambda x: (1 - (x['PRsh'] * x['PRsh'])) / ((((1 + x['PRsh']) * (1 - x['PRsh'] - (2 * (x['Esh_GPa'] / x['Esv_GPa']) * x['PRsv'] * x['PRsv'])))/(x['Esv_GPa'] * x['Esh_GPa'] * x['Esh_GPa'])) * x['Esh_GPa'] * x['Esh_GPa']), axis=1)
        
        #### Ask Santosh about NPHI
        input_df['Ad'] = input_df.apply(lambda x: x['RHOB'] / (1 - (0.1 / 1)), axis=1)    
        input_df['As'] = input_df.apply(lambda x: ((x['S44'] / x['S33']) + (x['S66'] / x['S11'])) / 2 if x[f'VP']>23000 else (1 / (1 - x['PRdva']))*((x['S44'] / x['S33']) + (x['S66'] / x['S11'])) / 2, axis=1)
        
        input_df['V_Biot'] = input_df.apply(lambda x: 1 - ((x['As']) * ((((2 * x['C13']) + x['C33']) / (3 * (((((((x['Ad'] * ((x['VP'] * 12 * 2.54 * 12 * 2.54 * x['VP']) - (4 * x['VS'] * 12 * 2.54 * 12 * 2.54 * x['VS'] / 3)))) / 1000000) / 68900) * 6.894757) + (((((x['Ad'] * ((x['Vp_90'] * 12 * 2.54 * 12 * 2.54 * x['Vp_90']) - (4 * x['Vs_90'] * 12 * 2.54 * 12 * 2.54 * x['Vs_90'] / 3)))) / 1000000) / 68900) * 6.894757)) / 2))))), axis=1)
        input_df['H_Biot'] = input_df.apply(lambda x: 1 - (x['As'] * ((x['C11'] + x['C12'] + x['C13']) / (3 * (((((((x['Ad'] * ((x['VP'] * 12 * 2.54 * 12 * 2.54 * x['VP']) - (4 * x['VS'] * 12 * 2.54 * 12 * 2.54 * x['VS'] / 3)))) / 1000000) / 68900) * 6.894757) + (((((x['Ad'] * ((x['Vp_90'] * 12 * 2.54 * 12 * 2.54 * x['Vp_90']) - (4 * x['Vs_90'] * 12 * 2.54 * 12 * 2.54 * x['Vs_90'] / 3)))) / 1000000) / 68900) * 6.894757)) / 2)))), axis=1)
    
        # write_file_to_datalake(f"Geo_Mech_Projects/Pangea/{project_val}/{well_val}.json", input_df)
        # return func.HttpResponse(f"Success", status_code=200)
    else:
        if req.form['model'] == 'pangea':
            geo_config = read_dataframe_from_datalake('Config_files/geo_config.json').to_dict()
        elif req.form['model'] == 'usone':
            geo_config = json.loads(req.form['config'])

        ######################################################################################################
        # Variable Inputs
        ######################################################################################################
        input_df['Vp_45'] = input_df.apply(lambda x: (geo_config['Vp45']['M']) * x['VP'] + geo_config['Vp45']['C'], axis=1)
        input_df['Vp_90'] = input_df.apply(lambda x: (geo_config['Vp90']['M']) * x['VP'] + geo_config['Vp90']['C'], axis=1)
        input_df['Vs_90'] = input_df.apply(lambda x: (geo_config['Vs90']['M']) * x['VS'] + geo_config['Vs90']['C'], axis=1)

        ######################################################################################################

        input_df['Vp_45_GPa'] = input_df.apply(lambda x: (((x['RHOB'] * x['Vp_45'] * x['Vp_45'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894 , axis=1)
        input_df['Vp_45_Sq_GPa'] = input_df.apply(lambda x: x['Vp_45_GPa'] * x['Vp_45_GPa'] , axis=1)

        input_df['S11'] = input_df.apply(lambda x:(((x['RHOB'] * x['Vp_90'] * x['Vp_90'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894, axis=1) 
        input_df['S33'] = input_df.apply(lambda x:(((x['RHOB'] * x['VP'] * x['VP'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894, axis=1) 
        input_df['S44'] = input_df.apply(lambda x:(((x['RHOB'] * x['VS'] * x['VS'] * 12 * 12 * 2.54 * 2.54) / 1000000) / 68900) * 6.894, axis=1) 
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
        

        input_df['Ad'] = input_df.apply(lambda x: x['RHOB'] / (1 - (0.1 / 1)), axis=1)    
        input_df['As'] = input_df.apply(lambda x: ((x['S44'] / x['S33']) + (x['S66'] / x['S11'])) / 2 if x[f'VP']>23000 else (1 / (1 - x['PRdva']))*((x['S44'] / x['S33']) + (x['S66'] / x['S11'])) / 2, axis=1)
        
        input_df['V_Biot'] = input_df.apply(lambda x: 1 - ((x['As']) * ((((2 * x['C13']) + x['C33']) / (3 * (((((((x['Ad'] * ((x['VP'] * 12 * 2.54 * 12 * 2.54 * x['VP']) - (4 * x['VS'] * 12 * 2.54 * 12 * 2.54 * x['VS'] / 3)))) / 1000000) / 68900) * 6.894757) + (((((x['Ad'] * ((x['Vp_90'] * 12 * 2.54 * 12 * 2.54 * x['Vp_90']) - (4 * x['Vs_90'] * 12 * 2.54 * 12 * 2.54 * x['Vs_90'] / 3)))) / 1000000) / 68900) * 6.894757)) / 2))))), axis=1)
        input_df['H_Biot'] = input_df.apply(lambda x: 1 - (x['As'] * ((x['C11'] + x['C12'] + x['C13']) / (3 * (((((((x['Ad'] * ((x['VP'] * 12 * 2.54 * 12 * 2.54 * x['VP']) - (4 * x['VS'] * 12 * 2.54 * 12 * 2.54 * x['VS'] / 3)))) / 1000000) / 68900) * 6.894757) + (((((x['Ad'] * ((x['Vp_90'] * 12 * 2.54 * 12 * 2.54 * x['Vp_90']) - (4 * x['Vs_90'] * 12 * 2.54 * 12 * 2.54 * x['Vs_90'] / 3)))) / 1000000) / 68900) * 6.894757)) / 2)))), axis=1)
 

    ##########################
    # Overburden
    #########################
    try:
        try:
            obg_check = req.form['obg_check']
            
            if obg_check == None or obg_check[0] == "":
                try:
                    input_df['Sv'] = input_df.apply(lambda x: float(req.form['overburden_val']) * x['TVD'], axis=1)
                except:
                    input_df['Sv'] = input_df.apply(lambda x: float(req.form['overburden_val']) * x['DEPTH'], axis=1)
            else:
                # req.form['overburden_max_val'] = overburden_max_val
                # req.form['overburden_min_val'] = overburden_min_val
                # req.form['overburden_avg_val'] = overburden_avg_val
                try:
                    input_df['Sv'] = input_df.apply(lambda x: float(req.form['overburden_avg_val']) * x['TVD'], axis=1)
                except:
                    input_df['Sv'] = input_df.apply(lambda x: float(req.form['overburden_avg_val']) * x['DEPTH'], axis=1)
        
        except:
            try:
                input_df['Sv'] = input_df.apply(lambda x: float(req.form['overburden_val']) * x['TVD'], axis=1)
            except:
                input_df['Sv'] = input_df.apply(lambda x: float(req.form['overburden_val']) * x['DEPTH'], axis=1)


    except:
        write_file_to_datalake(f"Geo_Mech_Projects/Pangea/{project_val}/{well_val}.json", input_df)
        return func.HttpResponse(f"Completed till velocity and static properties", status_code=201)
    ##########################
    # Pore Pressure
    #########################
    try:
        if req.form['pressure_model'] == 'pangea_pressure_bool':
            pressure_config = read_dataframe_from_datalake('Config_files/pressure_config.json', False)

            try:
                ramp_data = json.loads(req.form['ramp_data'])
                for ramp in ramp_data:
                    CalculateRamp(in_df, 'Vp_NCT', ramp[0], ramp[1])
            except:
                print("No Ramp")


            try:
                input_df['Vp_NCT'] = input_df.apply(lambda x: (float(pressure_config[x['Tops']]) * x['VP']) if x['Tops'] != '0' else None, axis=1)
            except:
                if len(tops_list) <= 2:
                    input_df['Vp_NCT'] = input_df.apply(lambda x: 1.025 * x['VP'], axis=1)
                elif len(tops_list) >= 5:
                    input_df['Vp_NCT'] = input_df.apply(lambda x: 1.1 * x['VP'], axis=1)
                else:
                    input_df['Vp_NCT'] = input_df.apply(lambda x: 1.075 * x['VP'], axis=1)

            try:
                input_df['Pp'] = input_df.apply(lambda x: x['Sv'] - ((x['Sv'] - (x['V_Biot'] * 0.43 * x['TVD'])) * ((x['VP'] / x['Vp_NCT']) ** 3)), axis=1)
                input_df['PpG'] = input_df.apply(lambda x: x['Pp'] / x['TVD'], axis=1)
            except:
                input_df['Pp'] = input_df.apply(lambda x: x['Sv'] - ((x['Sv'] - (x['V_Biot'] * 0.43 * x['DEPTH'])) * ((x['VP'] / x['Vp_NCT']) ** 3)), axis=1)
                input_df['PpG'] = input_df.apply(lambda x: x['Pp'] / x['DEPTH'] if x['DEPTH'] != 0 else 0, axis=1)


        elif req.form['pressure_model'] == 'usval_pressure_bool':
            pressure_config = json.loads(req.form['pressure_data'])

            input_df['PpG'] = input_df.apply(lambda x: float(pressure_config[x['Tops']]) if x['Tops'] != '0' else None, axis=1)

            try:
                input_df['Pp'] = input_df.apply(lambda x: x['PpG'] * x['TVD'], axis=1)
            except:
                input_df['Pp'] = input_df.apply(lambda x: x['PpG'] * x['DEPTH'], axis=1)


            # input_df['Vp_NCT'] = input_df.apply(lambda x: (float(pressure_config[x['Tops']]) * x['VP']) if x['Tops'] != '0' else None, axis=1)

            # try:
            #     input_df['Pp'] = input_df.apply(lambda x: x['Sv'] - ((x['Sv'] - (x['V_Biot'] * 0.43 * x['TVD'])) * ((x['VP'] / x['Vp_NCT']) ** 3)), axis=1)
            #     input_df['PpG'] = input_df.apply(lambda x: x['Pp'] / x['TVD'], axis=1)
            # except:
            #     input_df['Pp'] = input_df.apply(lambda x: x['Sv'] - ((x['Sv'] - (x['V_Biot'] * 0.43 * x['DEPTH'])) * ((x['VP'] / x['Vp_NCT']) ** 3)), axis=1)
            #     input_df['PpG'] = input_df.apply(lambda x: x['Pp'] / x['DEPTH'] if x['DEPTH'] != 0 else 0, axis=1)

        elif req.form['pressure_model'] == 'usmodel_pressure_bool':
            pressure_config = json.loads(req.form['pressure_data'])

            input_df['Vp_NCT'] = input_df.apply(lambda x: float(pressure_config[x['Tops']]['Vp_NCT']) if x['Tops'] != '0' else 0.0, axis=1)

            input_df['CC'] = input_df.apply(lambda x: (float(pressure_config[x['Tops']]['Compressibility Constant'])) if x['Tops'] != '0' else 0.0, axis=1)
            input_df['EC'] = input_df.apply(lambda x: (float(pressure_config[x['Tops']]['Exponential Constant'])) if x['Tops'] != '0' else 0.0, axis=1)

            try:
                input_df['Pp'] = input_df.apply(lambda x: x['Sv'] - ((x['Sv'] - (x['CC'] * 0.43 * x['TVD'])) * ((x['EC']) ** 3)), axis=1)
                input_df['PpG'] = input_df.apply(lambda x: x['Pp'] / x['TVD'], axis=1)
            except:
                input_df['Pp'] = input_df.apply(lambda x: x['Sv'] - ((x['Sv'] - (x['CC'] * 0.43 * x['DEPTH'])) * ((x['EC']) ** 3)), axis=1)
                input_df['PpG'] = input_df.apply(lambda x: x['Pp'] / x['DEPTH'] if x['DEPTH'] != 0 else 0, axis=1)
        
            in_df =  in_df.drop(columns=['CC', 'EC'], errors='ignore')
    except:
        write_file_to_datalake(f"Geo_Mech_Projects/Pangea/{project_val}/{well_val}.json", input_df)
        return func.HttpResponse("Completed till OverBurden", status_code=201)

    # return func.HttpResponse(f"Success", status_code=200)
    
    # # Pp_1=Sv-(Sv- V_Biot *0.43*Ovbn_Z)*Pow(Vp_M/Vp_NCT,3) 

    # # PpG_1=Pp_1/Ovbn_Z 
    # try:
    #     input_df['Pp'] = input_df.apply(lambda x: x['Sv'] - ((x['Sv'] - (x['V_Biot'] * 0.43 * x['TVD'])) * ((x['VP'] / x['Vp_NCT']) ** 3)), axis=1)
    #     input_df['PpG'] = input_df.apply(lambda x: x['Pp'] / x['TVD'], axis=1)
    # except:
    #     input_df['Pp'] = input_df.apply(lambda x: x['Sv'] - ((x['Sv'] - (x['V_Biot'] * 0.43 * x['DEPTH'])) * ((x['VP'] / x['Vp_NCT']) ** 3)), axis=1)
    #     input_df['PpG'] = input_df.apply(lambda x: x['Pp'] / x['DEPTH'] if x['DEPTH'] != 0 else 0, axis=1)

    ##########################
    # Stress
    #########################
    # post_obj['tectonic_val'] = tectonic_val
    # post_obj['tensile_check'] = tensile_check
    # post_obj['UCS_check'] = Ucs_check
    # post_obj['UCS_m_val'] = Ucs_m_val
    # post_obj['UCS_x_val'] = Ucs_x_val
    # post_obj['UCS_c_val'] = Ucs_c_val
    # post_obj['Shmin_check'] = Shmin_check
    # post_obj['Shmin_val'] = Shmin_val
    # post_obj['SHmax_check'] = SHmax_check
    # post_obj['SA_check'] = Sa_check

    # Shmin_1 = (Esh/Esv )*(PRsv /(1-PRsh))*(Sv-V_Biot*Pp_1)+(H_Biot*Pp_1) + Tectonic Stress 
    # Eff_Shmin_1= (Shmin_1-(V_Biot *Pp_1)) 
    # SHmax: 2*Shmin-(V.Biot*Pp)-(UCS/12)
    try:
        try:
            tectonic_val = float(req.form['tectonic_val'])
        except:
            tectonic_val = 0

        if req.form['UCS_check'] == None or req.form['UCS_check'][0] == "" or req.form['tensile_check'] == None or req.form['tensile_check'][0] == "":
            k = ("Not calculating UCS")
            # return func.HttpResponse(f"Completed till Pore Pressure properties", status_code=201)
        else:
            input_df['UCS'] = input_df.apply(lambda x: (float(req.form['UCS_m_val']) * x[req.form['UCS_x_val']]) + float(req.form['UCS_c_val']), axis=1)

        if req.form['Shmin_check'] == None or req.form['Shmin_check'][0] == "":
            return func.HttpResponse(f"Completed till Pore Pressure properties", status_code=201)
        else:
            input_df['Shmin'] = input_df.apply(lambda x: (x['Esh']/x['Esv']) * (x['PRsv'] / (1 - x['PRsh'])) * (x['Sv'] - (x['V_Biot'] * x['Pp'])) + (x['H_Biot'] * x['Pp']) + tectonic_val, axis=1)
        
        if req.form['Eff_Shmin_check'] == None or req.form['Eff_Shmin_check'][0] == "":
            k = ("Not calculating Eff Stress")
        else:
            input_df['Eff_Shmin'] = input_df.apply(lambda x: x['Shmin'] - (x['V_Biot'] * x['Pp']), axis=1)            

        if req.form['SHmax_check'] == None or req.form['SHmax_check'][0] == "":
            return func.HttpResponse(f"Completed till Pore Pressure properties", status_code=201)
        else:
            if req.form['tensile_check'] == None or req.form['tensile_check'][0] == "":
                input_df['SHmax'] = input_df.apply(lambda x: (2 * x['Shmin']) - (x['V_Biot'] * x['Pp']) - (x['UCS'] / 12), axis=1)
            else:
                input_df['SHmax'] = input_df.apply(lambda x: (2 * x['Shmin']) - (x['V_Biot'] * x['Pp']), axis=1)
            
            
        input_df['Anisotropy'] = input_df.apply(lambda x: x['Esh'] / x['Esv'], axis=1)

        if req.form['SA_check'] == None or req.form['SA_check'][0] == "":
            k = ("Not calculating Stress Anisotropy")
        else:
            input_df['Stress Anisotropy'] = input_df.apply(lambda x: x['SHmax'] / x['Shmin'], axis=1)
        
        
    except:
        write_file_to_datalake(f"Geo_Mech_Projects/Pangea/{project_val}/{well_val}.json", input_df)
        return func.HttpResponse(f"Completed till Pore Pressure properties", status_code=201)






    write_file_to_datalake(f"Geo_Mech_Projects/Pangea/{project_val}/{well_val}.json", input_df)
    return func.HttpResponse(f"Success", status_code=200)

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
