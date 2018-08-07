#from confluent_kafka import Producer
from kafka import KafkaProducer
import base64
import os
import shutil

dirt = "D:\\Kafka\EP-01-07728_0016"
#dirtfolder='/home/sid/Documents/'
p=KafkaProducer(bootstrap_servers='130.127.55.239:9092')
listoffiles=os.listdir(dirt)
number_files=len(listoffiles)
print(number_files)
num=1
number_files_str=str(number_files)
if not (os.path.exists(os.getcwd()+'/'+'meta')):
    os.makedirs(os.getcwd()+'/'+'meta')
metafile=os.getcwd()+'/'+'meta'+'/'+'meta.txt'
#Writing number of files, the folder name and name of the files into a Text file
f=open(metafile,"w")
f.close()
f=open(metafile,"a")
f.write(number_files_str)
f.write("\n")

folder=os.path.basename(os.path.normpath(dirt))
f.write(folder)
f.write("\n")

for files in sorted(listoffiles):
	f.write(files)
	f.write("\n")
f.close()
f1=open(metafile, "r")
for data in f1.read():
	p.send('meta', data.encode('utf-8'))
	print(data)
data="?"
p.send('meta', data.encode('utf-8'))
print(data)
p.flush()
f=open(metafile,"r")
f.readline()
f.readline()

while 1:
    i=0
    j=0
    line=f.readline()
    line=line.rstrip()
    topic=line
    if not line:
        break
    fileloc=os.path.join(dirt,line)
    with open(fileloc,"rb") as imageFile:
        fileread=imageFile.read()
        #b=bytearray(fileread)
        #print(b[3])
        b=base64.encodestring(fileread)
        #print(b)
        p.send('meta', b)
        #print("topic is:")
        #print(topic)
    print(topic)
    p.flush()
f.close()
'''image=open(fileloc,'rb')
        size=os.path.getsize(fileloc)
	size10kb=size/102400
	while j<size10kb:
		image.seek(i,0)
		image_read=image.read(102400)
		image_64_encode=base64.encodestring(image_read)
		p.send(topic, image_64_encode)
		i+=102400
		j+=1
'''

