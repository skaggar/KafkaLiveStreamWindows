from kafka import KafkaConsumer
import base64
import os
import shutil
dirt=os.getcwd()
metafile="MetaConsumer.txt"
meta=os.path.join(dirt,metafile)
fh=open(meta, "w")
fh.close()
c = KafkaConsumer('meta2', bootstrap_servers=['130.127.48.181:9092'], group_id="my_group")
print("Fetching Meta file")


detect=0
for msg in c:
    print(msg.topic)
    g=msg.value.decode('utf-8')
    if(g=="?"):
        break
    fh=open(meta,"a")
    fh.write(g)
    print("Received message: %s" % g)
    fh.close()
print("Fetched Meta file")
fh=open(meta,"r")
count=fh.readline()
count=count.rstrip()
count=int(count)
print(count)



folder=fh.readline()
folder=folder.rstrip()
print(folder)
dirt=os.path.join(dirt,folder+'copy')
print(dirt)
if not (os.path.exists(dirt)):
    os.makedirs(dirt)


num=0
while(num<count):
    num=num+1
    image=fh.readline()
    image=image.rstrip()
    print(num)
    print('running')
    image='1'+image
    print(image)
    c.subscribe([image])
    
    f=open(dirt+"/"+image,"w")
    f.close()
    f=open(dirt+"/"+image,"ab")
    for msg in c:
        print(msg.value)
        if(msg.value==b'?'):
            break
        f.write(base64.decodestring(msg.value))
    f.close()
