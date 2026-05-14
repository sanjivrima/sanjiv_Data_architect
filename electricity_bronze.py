from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    year,
    month,
    dayofmonth,
    lit
)
import requests
import uuid
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ****************
#loging configuration
#*****************
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)

# spark session
spark = SparkSession.builder \
    .appName("ElectricityMapsBronzeIngestion") \
    .getOrCreate()

# constant
API_TOKEN = "34bw4EyFKHBbxsPmNQkA"
BRONZE_PATH = "/data_lake/bronze"
REQUEST_TIMEOUT = 30

#API configuration
API_ENDPOINTS = {
    "carbon_intensity": {
        "url": "https://api.electricitymaps.com/v3/carbon-intensity/past",
        "params": {
            "zone": "IN-SO",
            "datetime": "2026-05-13T01:51:27.551Z"
        }
    }
}

#HTTP session with retry
session = requests.Session()

retry_strategy = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)

#ingest function
def ingest_api_data(dataset_name, api_config):
    try:
      logger.info(f"Starting ingestion for {dataset_name}")
      url = api_config["url"]
      params = api_config["params"]
      headers = {
        "auth-token": API_TOKEN
      }
      # api call
      response = session.get(
          url=url,
          headers=headers,
          params=params,
          timeout=REQUEST_TIMEOUT
      )
      # raise error for bad status
      response.raise_for_status()
      # raw response
      raw_json = response.text
      #ingestion id
      ingestion_id = str(uuid.uuid4())
      logger.info(f"Ingestion ID: {ingestion_id}")
      #create record
      raw_record = [{
          "raw_data": raw_json,
          "source_url": response.url,
          "ingestion_id": ingestion_id,
          "api_status_code": response.status_code
      }]
      df = spark.createDataFrame(raw_record)

      # metadata+partition - transformation chain
      bronze_df = (
          df
          .withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("year", year("ingestion_timestamp"))
          .withColumn("month", month("ingestion_timestamp"))
          .withColumn("day", dayofmonth("ingestion_timestamp"))
      )
      #output path
      output_path = f"{BRONZE_PATH}/{dataset_name}"
      logger.info(f"Writing to {output_path}")
      #write data
      (
          bronze_df
          .repartition(1)
          .write
          .mode("append")
          .partitionBy("year", "month", "day")
          .json(output_path)
      )

      logger.info(
        f"Successfully completed ingestion for {dataset_name}"
      )
    except requests.exceptions.RequestException as req_error:
      logger.error(
            f"API request failed for {dataset_name}: {req_error}"
      )
#main excution
for dataset, config in API_ENDPOINTS.items():
    ingest_api_data(dataset, config)
spark.stop()







