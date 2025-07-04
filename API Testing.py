# Databricks notebook source
import requests
import json

API_KEY = "88b1862508f7780614e4ca6542ef0d32"
CITIES = ["Johannesburg","Cape Town","Durban","London","Hong Kong","Rome","Amsterdam","San Francisco","Paris","New York","Sydney","Barcelona","Berlin","Seoul","Kuala Lumpur","Dubai","Prague","Tokyo","Instanbul","Shanghai","Los Angeles","Madrid","Toronto","Chicago","Vienna","Seattle","Milan","Boston","Vancouver","Miami"]

for CITY in CITIES:
    URL = f"http://api.openweathermap.org/data/2.5/forecast?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(URL)

    if response.status_code == 200:
        data = response.json()
       
        print(json.dumps(data, indent=4))
    else:
        print(f"Failed to fetch data for {CITY}: {response.status_code}")



