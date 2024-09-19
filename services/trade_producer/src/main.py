from quixstreams import Application
from typing import List, Dict
from src.kraken_api import KrakenWebsocketTradeAPI
from loguru import logger

def produce_trades(
        kafka_broker_address: str,
        kafka_topic_name: str,
        product_id: str
) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    Args:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_topic_name (str): The name of the Kafka topic.
         product_id: (str): The trade pair.

    Returns:
        None
    """
    app = Application(broker_address=kafka_broker_address)

    # the topic where we will save the trades
    topic = app.topic(name=kafka_topic_name, value_serializer='json')
    
    # create an instance of the Kraken API
    kraken_api = KrakenWebsocketTradeAPI(product_id=product_id)

    logger.info('Creating the producer...')

    # Create a Producer instance
    with app.get_producer() as producer:

        while True:

            # Get trades from the Kraken API and save them as a list of dictionaries
            trades : List[Dict] = kraken_api.get_trades()

            # access each dictionary saved in `trades`
            for trade in trades:
                # Serialize an event using the defined Topic 
                message = topic.serialize(key=trade["product_id"],
                                          value=trade)

                # Produce a message into the Kafka topic
                producer.produce(
                    topic=topic.name,
                    value=message.value,
                    key=message.key
                )
            
                logger.info('Message sent!')
            
            from time import sleep
            sleep(1)

if __name__ == '__main__':

    produce_trades(
        kafka_broker_address="redpanda-0:9092",
        kafka_topic_name="trade",
        product_id='BTC/USD'
    )