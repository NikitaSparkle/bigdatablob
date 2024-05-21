import requests
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

response = requests.get("https://jsonplaceholder.typicode.com/todos/")
todos_data = response.json()

# Step 3: Save the data to a JSON file
with open("/dbfs/user/hive/warehouse/download.json", "w") as json_file:
    json.dump(todos_data, json_file)

# Step 4: Upload the JSON file to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=bigdatalabblob;AccountKey=8CUUp0dUP9NF4eU0r9+7yY5MuWdY8RWTWCbQegfzyRKhnvb1e9Ke9V3IgkVgl31ZiE3/szvqVxa6+AStjaSCqQ==;EndpointSuffix=core.windows.net")
container_client = blob_service_client.get_container_client("bigdatalabblob")

blob_client = container_client.get_blob_client("todos_data.json")
with open("/dbfs/user/hive/warehouse/download.json", "rb") as data:
    blob_client.upload_blob(data, overwrite=True)
