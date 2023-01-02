from fastapi import FastAPI, UploadFile, Header
from pydantic import BaseModel

from pathlib import Path
import requests
import pandas as pd
from espo_api_client import EspoAPI
import os
import base64
from dotenv import load_dotenv
import click
import shutil
load_dotenv(dotenv_path=".env")

class KoboDrive(BaseModel):
    gdriveurl: str
    id: int
    class Config:
        fields = {'id':'_id'}

class Kobo(BaseModel):
    id: int
    class Config:
        fields = {'id':'_id'}

class Upload(BaseModel):
    forceOverwrite: str

app = FastAPI()
path = 'C:/Users/TZiere/Git/kobo2espoAPI'

def get_kobo_data_id(ASSET,ID,TOKEN):
    # Get data from kobo
    headers = {'Authorization': f'Token {TOKEN}'}
    data_request = requests.get(
        f'https://kobonew.ifrc.org/api/v2/assets/{ASSET}/data.json/?query={{"_id":{ID}}}',
        headers=headers)
    data = data_request.json()['results'][0]
    return data

@app.post("/kobo/{assetid}/uploadcsv")
async def create_upload_file(mappingcsv:UploadFile, assetid:str, overwrite:Upload | None = None):
    path = f'data/{assetid}/mapping.csv'
    isExist = os.path.isfile(path)
    print(overwrite)
    print(type(overwrite))
    if overwrite != None:
        print('test')
        overwrite = dict(overwrite)['overwrite']
    
    if (overwrite == None or overwrite == 'false'):    
        if (isExist == True):
            return f"file already exists, please use overwrite=true in your request body if you want to overwrite the existing file OR use a GET request on /kobo/{assetid}/mappingcsv to view current file"
    else:
        Path(f"data/{assetid}").mkdir(parents=True, exist_ok=True)
        fileLocation = f"data/{assetid}/mapping.csv"
        with open(fileLocation, "wb") as buffer:
            shutil.copyfileobj(mappingcsv.file, buffer)

        return {"Result": "OK",
                "filename": "mapping.csv",
                "assetid": assetid}
    print(overwrite)

@app.get("/kobo/{assetid}/mappingcsv")
async def create_upload_file(assetid:str):
    path = f'/data/{assetid}/mapping.csv'
    mappingcsv = pd.read_csv(path)
    return {mappingcsv}


@app.post("/kobo/{assetid}/gdrive")
async def kobo(assetid: str, kobo:KoboDrive, kobotoken: str = Header(default=None)):
    kobo = dict(kobo)
    url = kobo['gdriveurl']
    koboid = kobo['id']
    # Get latest KoBo Submission
    df = get_kobo_data_id(assetid,koboid,kobotoken)
    # remove group names
    for key in list(df.keys()):
        new_key = key.split('/')[-1]
        df[new_key] = df.pop(key)
    
    # Create a dataframe to map the Kobo question names to the Espo Fieldnames, mapping csv should be publicly on google drive
    url='https://drive.google.com/uc?id=' + url.split('/')[-2]
    mapping = pd.read_csv(url, header=0, index_col=0, squeeze=True)

    # Create API payload body
    payload = {}
    
    for ix, row in mapping.iterrows():
        field = row['esponame']  # field in espo
        question = row['koboname']  # question in kobo
        question_type = row['type']  # question type in kobo
        try:
            # If select_multiple questions, split up string, turn into list (array in json)
            if question_type == 'select_multiple':
                payload_value = df[question].split()
            # If no conditions apply, map right value
            else:
                payload_value = df[question]
        # If field is not filled in KoBo survey, pass empty string
        except KeyError:
            payload_value = ''
        payload[field] = payload_value

    return payload

@app.post("/kobo/{assetid}")
async def kobo(assetid: str, kobo:Kobo, kobotoken: str = Header(default=None)):
    kobo = dict(kobo)
    koboid = kobo['id']
    # Get latest KoBo Submission
    df = get_kobo_data_id(assetid,koboid,kobotoken)
    # remove group names
    for key in list(df.keys()):
        new_key = key.split('/')[-1]
        df[new_key] = df.pop(key)
    
    # Create a dataframe to map the Kobo question names to the Espo Fieldnames, mapping csv should be publicly on google drive
    mappingfile = f'/{assetid}/mapping.csv'
    mapping = pd.read_csv(mappingfile, header=0, index_col=0, squeeze=True)

    # Create API payload body
    payload = {}
    
    for ix, row in mapping.iterrows():
        field = row['esponame']  # field in espo
        question = row['koboname']  # question in kobo
        question_type = row['type']  # question type in kobo
        try:
            # If select_multiple questions, split up string, turn into list (array in json)
            if question_type == 'select_multiple':
                payload_value = df[question].split()
            # If no conditions apply, map right value
            else:
                payload_value = df[question]
        # If field is not filled in KoBo survey, pass empty string
        except KeyError:
            payload_value = ''
        payload[field] = payload_value

    return payload