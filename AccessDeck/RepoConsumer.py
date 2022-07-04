import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,StringType,FloatType,StructField,TimestampType,DoubleType

from AccessDeckAnalytics import ConsumerStreamData as csd






repo_schema={
"access_deck_id":StringType(),
"cloud_provider_id":StringType(),
"cloud_provider_name":StringType(),
"filesize":DoubleType(),
"remote_access":IntegerType(),
"record_ts":TimestampType(),
}

host= "127.0.0.1"
spark=SparkSession.builder.appName("ReadStreamData1").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

port=9997


#csd.streamToTbl(spark=spark,host=host,port=port,schema=connectionSchema,outPath="file:///E:/Devesh/AccessDeck/streamedData/tbl1")
csd.streamToTbl(spark=spark,host=host,port=port,format="parquet",schema=repo_schema,outPath="file:///E:/Devesh/AccessDeck/data/jrnl/repo")