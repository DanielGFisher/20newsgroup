from fastapi import FastAPI
import threading
from consumer import Consumer

app = FastAPI()

consumer_interesting = Consumer("interesting", group_id="group1")
consumer_not_interesting = Consumer("not_interesting", group_id="group2")

@app.on_event("startup")
def start_consumers():
    threading.Thread(target=consumer_interesting.consume_messages, daemon=True).start()
    threading.Thread(target=consumer_not_interesting.consume_messages, daemon=True).start()

@app.get("/messages/{topic}")
def get_messages(topic: str):
    if topic == "interesting":
        return {"messages": consumer_interesting.get_messages()}
    elif topic == "not_interesting":
        return {"messages": consumer_not_interesting.get_messages()}
    else:
        return {"error": "Invalid topic"}
