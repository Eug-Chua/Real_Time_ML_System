import os
from dotenv import load_dotenv, find_dotenv

# load my .env file variables as environment variables
# with os.environm[] statements
load_dotenv(find_dotenv())

from pydantic_settings import BaseSettings

class Config(BaseSettings):
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    kafka_input_topic_name: str = "trade"
    kafka_output_topic_name: str = "ohlc"
    ohlc_windows_seconds: int = os.environ['OHLC_WINDOWS_SECONDS']

config = Config()