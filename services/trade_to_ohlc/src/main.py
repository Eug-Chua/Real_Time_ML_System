from datetime import timedelta
from loguru import logger
from src.config import config

def trade_to_ohlc(
        kafka_input_topic: str,
        kafka_output_topic: str,
        kafka_broker_address: str,
        olhc_window_seconds: int
) -> None:
    """
    Reads trades from the kafka input topic
    Aggregates them into OHLC candles using the specified window in `olhc_window_seconds`
    Saves the ohlc data info into another kafka topic

    Args:
        kafka_input_topic: str : Kafka topic to read trade data from
        kafka_output_topic: str : Kafka topic to read trade data to
        kafka_broker_address: str : Kafka broker address
        olhc_window_seconds: int : Window size in seconds for OHLC aggregation
    """
    from quixstreams import Application

    # this handles all the low level communcation with kafka
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="trade_to_ohlc",
        auto_offset_rest='earliest'
    )

    # specify the input and output topics for this application
    input_topic = app.topic(name=kafka_input_topic, value_serializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    # create a streaming dataframe
    sdf = app.dataframe(topic=input_topic)

    def init_ohlc_candle(value:dict) -> dict:
        """
        Initialize the OHLC candle with the first trade
        """
        return {
            # "timestamp":value['timestamp'],
            "open": value['price'],
            "high": value['price'],
            "low": value['price'],
            "close": value['price'],
            'product_id': value['product_id']
        }

    def update_ohlc_candle(ohlc_candle: dict, trade: dict) -> dict:
        """
        Update the OHLC candle with the new trade and return the updated candle

        Args: 
            ohlc_candle : dict : the current OHLC candle
            trade : dict : the incoming trade

        Returns:
            dict : the updated OHLC candle
        """

        return {
            "open": ohlc_candle['open'],
            "high": max(ohlc_candle['high'], trade['price']),
            "low": min(ohlc_candle['low'], trade['price']),
            "close": trade['price'],
            'product_id': trade['product_id']
        }

    # apply transformation to the incoming data
    sdf = sdf.tumbling_window(duration_ms=timedelta(seconds=olhc_window_seconds))
    sdf = sdf.reduce(reducer=update_ohlc_candle, initializer=init_ohlc_candle).current()

    # unpack the values we want 
    sdf['open'] = sdf['value']['open']
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf['close'] = sdf['value']['close']
    sdf['product_id'] = sdf['value']['product_id']

    # add timestamp key
    sdf['timestamp'] = sdf['end']
    

    sdf = sdf.update(logger.info)

    sdf = sdf.to_topic(output_topic)

    # kick-off the streaming application
    app.run(sdf)

if __name__ == '__main__':

    trade_to_ohlc(
        kafka_input_topic=config.kafka_input_topic_name,
        kafka_output_topic=config.kafka_output_topic_name,
        kafka_broker_address=config.kafka_broker_address,
        olhc_window_seconds=config.ohlc_window_seconds
    )