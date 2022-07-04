from AccessDeckAnalytics import ProducerStreamData as psd



#data transfer:access_deck_id,cloud_provider_id,cloud_provider_name,file_size,status,reason,file_location,timed_uploads_freq,upload_time,record_ts
datatransfer_schema={
"access_deck_id":"int_value:1,100000",
"cloud_provider_id":"int_value:1,100000",
"cloud_provider_name":["onedrive","googledrive","icloud","dropbox"],
"filesize":"int_value:1,99999999",
"failure_reason":['101','102','200'],
"file_location":"alphanumeric_value:12",
"location":["india.gujarat","usa.florida","india.delhi","india.kerala","usa.arizona",'NULL'],
"timed_uploads_freq":['ontime','daily','weekly','month'],
"upload_time":"timestamp_value:1,30",
"record_ts":"timestamp_value:1,30",

}
host= "127.0.0.1"
port=9998
psd.sendRandomData(schema=datatransfer_schema,host =host,port =port,wait=1,interval=100)
