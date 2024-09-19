from typing import List, Dict
import json
from websocket import create_connection
from loguru import logger

class KrakenWebsocketTradeAPI:
    
    URL = 'wss://ws.kraken.com/v2'

    def __init__(
            self,
            product_id: str
            ):
        
        self.product_id = product_id

        # establish connection to the Kraken websocket API
        self._ws = create_connection(self.URL)
        logger.info('Connection established')

        # subscribe to the trades for the given `product_id`
        self._subscribe(product_id)

    def _subscribe(self, product_id: str):
        """
        Establish connection to Kraken websocket API and
        subscribe to the trades for the given `product_id
        """
        logger.info(f'Subscribing to trades for {product_id}')

        # subscribe to the trades for the given `product_id`
        msg = {
            "method": "subscribe",
            "params": {
                "channel": "trade",
                "symbol": [product_id],
                "snapshot": False
                }
            }
            
        self._ws.send(json.dumps(msg))
        logger.info('Subscription worked!')

        # putting aside the first 2 messages we get from the websocket because
        # they contain no trade data, just confirmation on their end that
        # subscription was successful
        _ = self._ws.recv()
        _ = self._ws.recv()

    def get_trades(self) -> List[Dict]:
        
        # mock_trades = [
        #     {
        #         'product_id': 'BTC-USD',
        #         'price': 60000,
        #         'volume': 0.01,
        #         'timestamp': 1630000000
        #     },

        #     {
        #         'product_id': 'BTC-USD',
        #         'price': 59000,
        #         'volume': 0.01,
        #         'timestamp': 1640000000
        #     }
        # ]

        message = self._ws.recv()

        if 'hearbeat' in message:
            # when we get a hearbeat, return an empty list
            return[]
        
        # parse the message strings as a dictionary
        message = json.loads(message)
        
        # Check if 'data' key exists in the message
        if 'data' not in message:
            logger.info("Received message does not contain data.")
            return []

        # Extract trades from the message['data'] field
        trades = []
        for trade in message['data']:
            trades.append({
                'product_id': self.product_id,
                'price': trade['price'],
                'volume': trade['qty'],
                'timestamp': trade['timestamp']
            })

        return trades

