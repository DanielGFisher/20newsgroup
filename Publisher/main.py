from fastapi import FastAPI
from sklearn.datasets import fetch_20newsgroups
import random
from producer import Producer

app = FastAPI()
producer = Producer(broker="kafka:9092")

interesting_categories = [
    "alt.atheism","comp.graphics","comp.os.ms-windows.misc",
    "comp.sys.ibm.pc.hardware","comp.sys.mac.hardware",
    "comp.windows.x","misc.forsale","rec.autos",
    "rec.motorcycles","rec.sport.baseball"
]

not_interesting_categories = [
    "rec.sport.hockey","sci.crypt","sci.electronics","sci.med",
    "sci.space","soc.religion.christian","talk.politics.guns",
    "talk.politics.mideast","talk.politics.misc","talk.religion.misc"
]

newsgroups_interesting = fetch_20newsgroups(subset="train", categories=interesting_categories)
newsgroups_not_interesting = fetch_20newsgroups(subset="train", categories=not_interesting_categories)

@app.get("/publish")
def publish_messages():
    for _ in range(5):
        msg = random.choice(newsgroups_interesting.data)
        producer.send_message("interesting", {"text": msg, "category": "interesting"})
    for _ in range(5):
        msg = random.choice(newsgroups_not_interesting.data)
        producer.send_message("not_interesting", {"text": msg, "category": "not_interesting"})
    return {"status": "10 messages published"}
