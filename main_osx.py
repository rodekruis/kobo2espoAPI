from fastapi import FastAPI, UploadFile, Header, HTTPException
from pydantic import BaseModel

import requests
import pandas as pd
from espo_api_client import EspoAPI
import os
import base64
from dotenv import load_dotenv
import click
import shutil
load_dotenv(dotenv_path=".env")

class Create(BaseModel):
    projectname: str

class config(BaseModel):
    koboToken: str
    koboAsset: str
    espoURL: str


class KoboDrive(BaseModel):
    gdriveurl: str
    id: int
    class Config:
        fields = {'id':'_id'}

class Kobo(BaseModel):
    id: int
    class Config:
        fields = {'id':'_id'}


app = FastAPI()

#dir = 'C:/Users/TZiere/Git/kobo2espoAPI'
dir = '/Users/tijsziere/Documents/GitHub/kobo2espoAPI/'

def get_kobo_data_id(ASSET,ID,TOKEN):
    # Get data from kobo
    headers = {'Authorization': f'Token {TOKEN}'}
    data_request = requests.get(
        f'https://kobonew.ifrc.org/api/v2/assets/{ASSET}/data.json/?query={{"_id":{ID}}}',
        headers=headers)
    data = data_request.json()['results'][0]
    return data

@app.post("/create/{projectname}")
async def create_project(projectname:str):
    path = os.path.join(dir, projectname)
    try: 
        os.mkdir(path)
        return {"Result": "OK", "projectname": projectname}
    except OSError as error: 
        raise HTTPException(status_code=422, detail="Project already exists")

@app.post("/{projectname}/csv")
async def upload_csv_file(projectname: str, mappingcsv: UploadFile):
    path = os.path.join(dir, projectname)
    with open(mappingcsv.filename, "wb") as buffer:
        shutil.copyfileobj(f'{path}/{mappingcsv.file}', buffer)
    return {"Result": "OK", "filename": mappingcsv.filename}

@app.get("{projectname}/csv")
async def view_csv_file(projectname: str):
    path = os.path.join(dir, projectname)
    mapping = pd.read_csv(f'{path}/{mappingcsv.file}')
    return mapping

@app.post("/{projectname}/config")
async def configure_project():
    return {"Result": "OK"}

@app.post("/kobo/{assetid}")
async def kobo(assetid: str, kobo:Kobo, kobotoken: str = Header(default=None), mappingfile: str = Header(default=None)):
    kobo = dict(kobo)
    koboid = kobo['id']
    # Get latest KoBo Submission
    df = get_kobo_data_id(assetid,koboid,kobotoken)
    # remove group names
    for key in list(df.keys()):
        new_key = key.split('/')[-1]
        df[new_key] = df.pop(key)
    
    # Create a dataframe to map the Kobo question names to the Espo Fieldnames, mapping csv should be publicly on google drive
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

@app.post("/kobo/gdrive/{assetid}")
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