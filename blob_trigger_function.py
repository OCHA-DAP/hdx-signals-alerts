import azure.functions as func
import logging
import requests

app = func.FunctionApp()


@app.blob_trigger(arg_name="myblob",
                  path="path_STORAGE",
                  connection="blob_STORAGE")
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                 f"Name: {myblob.name}"
                 f"Blob Size: {myblob.length} bytes")
    url = 'https://api.github.com/repos/OCHA-DAP/hdx-signals-alerts/.github/workflows/dispatches'
    payload = open("request.json")
    headers = {'Accept': 'application/vnd.github.v3+json'}
    r = requests.post(url, data=payload, headers=headers)
    logging.info(f"Python blob trigger function processed blob"
                 f"Name: {myblob.name}"
                 f"Blob Size: {myblob.length} bytes")
