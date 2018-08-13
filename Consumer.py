from kafka import KafkaConsumer
import base64
import os
import time
import sys

#c = KafkaConsumer('meta10', bootstrap_servers=['130.127.55.239:9092'], group_id="my_group")
c = KafkaConsumer('meta', bootstrap_servers=['130.127.48.181:9092'], group_id="my_group")
#c=KafkaConsumer(consumer_timeout_ms=1000)
running = True
num=0
topics=[]
detect=0
dirt= os.getcwd()
metafile="MetaConsumer.txt"
'''
meta=os.path.join(dirt,metafile)
#fh=open("/home/sid/Documents/NewFile1.txt","w")
fh=open(meta, "w")
fh.write("")
fh.close()
print("Fetching Meta file")
#print(c.subscription())
while running:
    #msg = c.poll()
    batch=c.poll()
    for msg in c:
        #print(partition)
        #print(c)
        print(msg.topic)
        g=msg.value.decode('utf-8')
        if(g=='?'):
            detect+=1
            #if detect==15:
            break
        #fh=open("/home/sid/Documents/NewFile1.txt","a")
        fh=open(meta, "a")
        fh.write(g)
        print('Received message: %s' % g)
        fh.close()
    if (detect==1):
        break
        
#c.unsubscribe()
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
#c.close()
'''
meta=os.path.join(dirt,metafile)
fh=open(meta,"w")
fh.close()
fh=open(meta,"r")
count=fh.readline()
count=count.rstrip()
count=1
folder=fh.readline()
folder=folder.rstrip()
while (num<count):
    num=num+1
    image=fh.readline()
    image=image.rstrip()
    #c = KafkaConsumer()
    #d = KafkaConsumer(bootstrap_servers=['130.127.55.239:9092'])
    #c.subscribe([image])
    print(num)
    print('running')
    r=0
    #d=subscribe(topics=[image])

    print(c.subscription())
    detect=0
    print(image)
    #print(d)
    #msg=d
    #print(msg.topic)
    batch=c.poll()
    for msg in c:
        print(msg.topic)
        #image_64_decode=base64.decodestring(msg.value)
        #if(msg.value==b''):
        #    break
        
        '''
        print("hi")
        r=r+1
        print(msg.topic)
        filename=os.path.join(dirt,image)
        #msg=c.poll()
        g=msg.value
        if(g==b''):
            break
        #image_64_decode=base64.decodestring(g)

        image_result=open(filename, 'ab')
        print("Writing")
        print(num)
        print(r)
        image_result.write(g)
        #image_result.write(image_64_decode)
        image_result.close()
        print(g)
        #print(image_64_decode)
        '''
    c.unsubscribe()
c.close()
