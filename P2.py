from kafka import KafkaProducer
import base64
import os
import shutil
dirt=os.getcwd()
dirt=dirt+"/"+"EP-01-07728_0016"
p=KafkaProducer(bootstrap_servers='130.127.48.181:9092')


listoffiles=os.listdir(dirt)
number_files=len(listoffiles)
number_files_str=str(number_files)
print("Number of files are " +str(number_files))
if not (os.path.exists(os.getcwd()+'/'+'meta')):
    os.makedirs(os.getcwd()+'/'+'meta')
metafile=os.getcwd()+'/'+'meta'+'/'+'meta.txt'


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
    p.send('meta2', data.encode('utf-8'))
    print(data)
data="?"
p.send('meta2',data.encode('utf-8'))
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
    image=open(fileloc,"rb")
    size=os.path.getsize(fileloc)
    size10kb=size/10240
    while(j<=size10kb):
        image.seek(i,0)
        image_read=image.read(10240)
        image_64_encode=base64.encodestring(image_read)
        p.send('1'+topic,image_64_encode)
        i+=10240
        j+=1
    data='?'
    p.send('1'+topic,data.encode('utf-8'))
    p.flush()
f.close()
