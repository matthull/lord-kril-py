import os
import pdb
import sys
from kafka import SimpleProducer, KafkaClient
import requests
import websocket
from functools import partial

#######################
#  Stockfighter API   #
#######################
def base_uri(protocol = 'https://'):
    return protocol + "api.stockfighter.io/ob/api"

def ticker_url(account, venue):
    return '/'.join([base_uri("wss://") ,"ws", account, "venues", venue, "tickertape"])

######################
# Websocket Handlers #
######################
def on_message(topic, ticker, message):
    kafka = KafkaClient('localhost:9092')
    producer = SimpleProducer(kafka)
    producer.send_messages(topic.encode('utf-8'), message.encode('utf-8'))

def on_error(ticker, error):
    print("TICKER ERROR: " + str(error))

def on_close(ticker):
    print("TICKER CLOSED")

######################
#    Entry Point     #
######################
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: {0} account venue".format(os.path.basename(__file__)))
        sys.exit(1)
    account = sys.argv[1]
    venue   = sys.argv[2]
    topic   = "-".join(["ticker", account, venue])

    websocket.enableTrace(True)
    ticker = websocket.WebSocketApp(ticker_url(account, venue),
                                    on_message = partial(on_message, topic),
                                    on_error   = on_error,
                                    on_close   = on_close
                                    )
    ticker.run_forever()
