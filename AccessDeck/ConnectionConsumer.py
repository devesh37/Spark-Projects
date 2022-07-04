import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,StringType,FloatType,StructField,TimestampType

from AccessDeckAnalytics import ConsumerStreamData as csd



connectionSchema={
"access_deck_id":StringType(),
"cloud_provider_id":StringType(),
"cloud_provider_name":StringType(),
"location":StringType(),
"record_ts":TimestampType(),
"status":IntegerType()
}
host= "127.0.0.1"
spark=SparkSession.builder.appName("ReadConnectionStreamData").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

port=9999
csd.streamToTbl(spark=spark,host=host,port=port,format="parquet",schema=connectionSchema,outPath="file:///E:/Devesh/AccessDeck/data/jrnl/connection")




