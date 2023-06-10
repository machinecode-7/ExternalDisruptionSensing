from fastapi import FastAPI
from newsApi import create_data
import pandas as pd
import json


with open("pipeline_configuration.json") as pipeline_config_fd:
        pipeline_config = json.load(pipeline_config_fd)
        API_KEY = pipeline_config["NewsAPI"]["api_key"]
        from_date = pipeline_config["NewsAPI"]["from_date"]
        to_date = pipeline_config["NewsAPI"]["to_date"]


app = FastAPI()


@app.get("/")
def read_root():
    return "Healthcheck:Success"

@app.get("/dashboard")
def read_eds():
    data = pd.read_csv('report.csv')
    return data.to_dict(orient="records")

@app.get("/refresh")
async def create_eds():
    return await create_data(from_date, to_date, API_KEY)

