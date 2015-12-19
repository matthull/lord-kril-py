from kafka import SimpleProducer, KafkaClient
import requests
import websocket
import pdb

def base_uri(protocol = 'https://'):
    return protocol + "api.stockfighter.io/ob/api"

def ticker_url(account, venue):
    return '/'.join([base_uri("wss://") ,"ws", account, "venues", venue, "tickertape"])

def api_alive():
    heartbeat = requests.get(api_url('heartbeat'))
    return heartbeat.status_code == 200

kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

account = "HAR65741954"
venue = "XEJKEX"
topic = "-".join(["ticker", account, venue])

def on_message(ticker, message):
    producer.send_messages(topic.encode('utf-8'), message.encode('utf-8'))

def on_error(ticker, error):
    print("TICKER ERROR: " + str(error))

def on_close(ticker):
    print("TICKER CLOSED")

websocket.enableTrace(True)
ticker = websocket.WebSocketApp(ticker_url(account, venue),
                                on_message = on_message,
                                on_error   = on_error,
                                on_close   = on_close
                                )
ticker.run_forever()
