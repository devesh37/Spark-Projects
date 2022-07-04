import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,StringType,FloatType,StructField,TimestampType,DoubleType

from AccessDeckAnalytics import ConsumerStreamData as csd





datatransfer_schema={
"access_deck_id":StringType(),
"cloud_provider_id":StringType(),
"cloud_provider_name":StringType(),
"filesize":DoubleType(),
"failure_reason":IntegerType(),
"file_location":StringType(),
"location":StringType(),
"timed_uploads_freq":StringType(),
"upload_time":TimestampType(),
"record_ts":TimestampType(),
}

host= "127.0.0.1"
spark=SparkSession.builder.appName("ReaddatatransferStreamData").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

port=9998


csd.streamToTbl(spark=spark,host=host,port=port,format="parquet",schema=datatransfer_schema,outPath="file:///E:/Devesh/AccessDeck/data/jrnl/datatransfer")