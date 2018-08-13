from kafka import KafkaProducer
import base64
import os
import shutil
dirt=os.getcwd()
dirt=dirt+"/"+"apple.jpg"
p=KafkaProducer(bootstrap_servers='130.127.48.181:9092')
image=open(dirt,"rb")
size=os.path.getsize(dirt)
size10kb=size/10240
i=0
j=0
while j<size10kb:
    image.seek(i,0)
    image_read=image.read(10240)
    image_64_encode=base64.encodestring(image_read)
    p.send('t2', image_64_encode)
    i+=10240
    j+=1
    
with open(dirt,"rb") as imagefile:
    str=base64.encodestring(imagefile.read())
    #p.send('t1',str)
    print(str)

p.flush()
