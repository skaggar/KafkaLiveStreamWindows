from kafka import KafkaConsumer
import base64
import os
import shutil
dirt=os.getcwd()
dirt=dirt+"/consumer/"+"apple.jpg"
c = KafkaConsumer('t2', bootstrap_servers=['130.127.48.181:9092'], group_id="my_group")
f=open(dirt,"wb")
for msg in c:
    print(msg.value)
    f.write(base64.decodestring(msg.value))
f.close()
