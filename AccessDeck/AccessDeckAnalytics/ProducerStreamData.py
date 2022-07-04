#!/usr/bin/python3           # This is server.py file
import socket
import time    
import random
import datetime
import secrets
import string
def generate_data(schema=None,total_record=10):
    record_list=None
    for j in range(total_record):
        record=None
        for col in schema.keys():
            value=schema[col]
            random_value=None
            if isinstance(value, str) and "int_value" in value:
                random_value=list(map(lambda x:int(x),value.split(":")[1].split(",")))
                random_value=random.randint(random_value[0],random_value[1])
            elif isinstance(value, str) and "timestamp_value" in value:
                now=datetime.datetime.utcnow().replace(microsecond=0)
                random_value=list(map(lambda x:int(x),value.split(":")[1].split(",")))
                random_value=now-datetime.timedelta(days =random.randint(random_value[0],random_value[1]) )-datetime.timedelta(hours=random.randint(random_value[0],random_value[1]) )
            elif isinstance(value, str) and "alphanumeric_value" in value:
                N=int(value.split(":")[1])
                random_value = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(N))
            elif isinstance(value, list):
                random_value=value[random.randint(0,len(value)-1)]
            field=str(col)+"="+str(random_value)
            if record==None:
                record=field
            else:
                record=record+","+field

        if record_list==None:
            record_list=record
        else:
            record_list=record_list+"|"+record   
        #print(record)
    return record_list


def sendRandomData(schema=None,host = socket.gethostname(),port = 9999,wait=1,interval=999999):
   serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   serversocket.bind((host, port))    
   serversocket.listen(5) 
   i=0
   print("Listening")
   clientsocket,addr = serversocket.accept()  
   
   while i<interval:
   # establish a connection
      print("Client addr:", str(addr))
      #msg=str(random.randint(3, 100000))+" "+str(random.randint(3, 100000))+"\n"
      records=generate_data(schema=schema,total_record=10)+"\n"
      print(records)
      clientsocket.send(records.encode('ascii'))
      time.sleep(wait)
      i=i+1
   time.sleep(100)
   clientsocket.close()
   print("record_count:",i)

'''
host= "127.0.0.1"
connection={
"access_deck_id":"int_value:1,100000",
"cloud_provider_id":"int_value:1,100000",
"cloud_provider_name":["onedrive","googledrive","icloud","dropbox"],
"record_ts":"timestamp_value:1,30",
"status":[1,0]
}
sendRandomData(schema=connection,host =host,port = 9999,wait=1)'''



