from kafka import KafkaConsumer
import json

topics = ["4UPUqMRk7TRMgml", "81aHJ1q11NBPMrL", "9kRcWv60rDACzjR", "Et9kgGMDl729KT4","IQ2d7wF4YD8zU1Q", "LYwnQax7tkwH5Cb", "LlT2YUhhzqhg5Sw", "Mx2yZCDsyf6DPfv","NgDl19wMapZy17u", "PeE6FRyGXUgsRhN", "Qf4GUc1pJu5T6c6", "Quc1TzYxW2pYoWX","V94E5Ben1TlhnDV", "WcxssY2VbP4hApt", "mqwcsP2rE7J0TFp", "oZ35aAeoifZaQzV", "oZZkBaNadn6DNKz", "q49J1IKaHRwDQnt", "rrq4fwE8jgrTyWY", "vOuJvMaM2sgwLmb","xMbIugepa2P7lBB", "xoJJ8DcxJEcupym"]

# consumer = KafkaConsumer(*topics, bootstrap_servers=['localhost:9092'],
#                          value_deserializer=lambda x: json.loads(x.decode('utf-8')))

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])

consumer.subscribe(topics)

# consume messages from the topic
for message in consumer:
    topic=message.topic
    data=json.loads(message.value.decode('utf-8'))
    total_yield=data['TOTAL_YIELD']
    print(f"Received message from topic {topic}: {total_yield}")
    
# close the consumer connection
consumer.close()
