import json
import logging

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError


def checkBlobandUpload(blobname, event, time):
    STORAGEACCOUNTURL = "https://adlszeus.blob.core.windows.net"
    STORAGEACCOUNTKEY = "ksL9a2OZFCiKFYPn6hzTNJcY4WI2Nq2xSsRlUD8cDH3dBBEvePAhJqErSP6QKN27so/2ayW3DnO7O8s4uPtUZA=="
    CONTAINERNAME = "pangea-velocity-app"
    BLOBNAME = blobname
    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)
    if blob_client_instance.exists():
        try:
            current_log_file = json.loads(blob_client_instance.download_blob().readall())
            logging.info(f"this is current file from blob {current_log_file}")
        except ResourceNotFoundError:
            current_log_file = {}
        current_log_file[time] = {'user':event.get_json()['user'],'hashKey':event.get_json()['hashKey'],'message':event.get_json()['message']}
        updated_log_file = json.dumps(current_log_file)
        try:
            blob_client_instance.upload_blob(updated_log_file, overwrite=True)
            logging.info(f"this is updated conent {updated_log_file}")
        except ResourceExistsError:
            logging.error("Resource exists (concurrence issue) and need to retry")
    else:
        log_data = {time:{'user':event.get_json()['user'],'hashKey':event.get_json()['hashKey'],'message':event.get_json()['message']}}
        blob_client_instance.upload_blob(data=json.dumps(log_data),  overwrite=True)
        logging.info(f"new log file created with path {blobname}")
    blob_client_instance.close()


def main(event: func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })
    user = event.get_json()['user']
    hashKey = event.get_json()['hashKey']
    date = event.get_json()['date']
    time = event.get_json()['time']

    # logging.info('Python EventGrid trigger processed an event new: %s', result)
    
    if (user and hashKey):
        logging.info("user and hash key persent")
        checkBlobandUpload(f"Log/{user}/{hashKey}/{date}/log.json", event, time)
    else:
        logging.info("hashKey not present")
