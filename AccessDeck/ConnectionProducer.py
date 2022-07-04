from AccessDeckAnalytics import ProducerStreamData as psd



#connection:access_deck_id,cloud_provider_id,cloud_provider_name,Timestamp,status
connection={
"access_deck_id":"int_value:1,100000",
"cloud_provider_id":"int_value:1,100000",
"cloud_provider_name":["onedrive","googledrive","icloud","dropbox"],
"location":["india.gujarat","usa.florida","india.delhi","india.kerala","usa.arizona"],
"record_ts":"timestamp_value:1,30",
"status":[1,0]
}
host= "127.0.0.1"
psd.sendRandomData(schema=connection,host =host,port = 9999,wait=1,interval=100)
