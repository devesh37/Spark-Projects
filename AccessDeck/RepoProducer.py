from AccessDeckAnalytics import ProducerStreamData as psd



#data transfer:access_deck_id,cloud_provider_id,cloud_provider_name,filesize,location,remote_access
repo_schema={
"access_deck_id":"int_value:1,100000",
"cloud_provider_id":"int_value:1,100000",
"cloud_provider_name":["onedrive","googledrive","icloud","dropbox"],
"filesize":"int_value:1,99999999",
"remote_access":[1,0],
"record_ts":"timestamp_value:1,30",
}
host= "127.0.0.1"
port=9997
psd.sendRandomData(schema=repo_schema,host =host,port =port,wait=1,interval=100)
