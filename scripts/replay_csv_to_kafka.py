import pandas as pd, json, time
from kafka import KafkaProducer

df = pd.read_csv("data/raw/online_retail_II.csv", encoding="latin1")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i, row in df.iterrows():
    message = row.dropna().to_dict()
    producer.send("retail_transactions", value=message)
    if i % 500 == 0:
        print(f"Envoyé : {i} lignes")
    time.sleep(0.01)

producer.flush()
print("Terminé.")