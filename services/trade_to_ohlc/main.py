def trade_to_ohlc(
        kafka_input_topic: str,
        kafka_output_topic: str,
        kafka_broker_address: str,
        olhc_window_seconds: int
) -> None:
    """
    Reads trades from the kafka input topic
    Aggregates theminto OHLCK candles using the specified window in `olhc_window_seconds`
    Saves the ohlc data info into another kafka topic

    Args:
        kafka_input_topics: str : Kafka topic to read trade data from
        kafka_output_topics: str : Kafka topic to read trade data to
        kafka_broker_address: str : Kafka broker address
        olhc_window_seconds: int : Window size in seconds for OHLC aggregation
    """
    from quixstreams import Application

    # this handles all the low level communcation with kafka
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="trade_to_ohlc"
    )

    # specify the input and output topics for this application
    input_topic = app.topic(name=kafka_input_topic, value_serializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    # create a streaming dataframe
    sdf = app.dataframe(topic=input_topic)

    # apply transformation to the incoming data

    sdf = sdf.to_topic(output_topic)

    # kick-off the streaming application
    app.run(sdf)

if __name__ == '__main__':

    # read environment varaibles
    kafka_input_topic = os.getenv('KAFKA_INPUT_TOPIC')
    kafka_output_topic = os.getenv('KAFKA_OUTPUT_TOPIC')
    kafka_broker_address = os.getenv('KAFKA_BROKER_ADDRESS')
    olhc_window_seconds = os.getenv('OHLC_WINDOW_SECONDS')

    trade_to_ohlc(
        kafka_input_topic=kafka_input_topic,
        kafka_output_topic=kafka_output_topic,
        kafka_broker_address=kafka_broker_address,
        olhc_window_seconds=olhc_window_seconds
    )