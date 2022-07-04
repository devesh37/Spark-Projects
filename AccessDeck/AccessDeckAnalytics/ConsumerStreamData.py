#import findspark
#findspark.init()
#from pyspark import SparkContext
#from pyspark.sql import SparkSession
#from pyspark.sql.types import StructType,IntegerType,StringType,FloatType,StructField,TimestampType
from pyspark.streaming import StreamingContext
from datetime import datetime
from pyspark.sql.functions import lit,col


record_count=0
def parseStream(record, spark,schema=None,outPath=None,mode="overwrite",format="csv",show=True):
    if not record.isEmpty():
        df = spark.createDataFrame(record) 
        
        now = datetime.utcnow()
        batch_date = now.strftime("%Y-%m-%d")
        batch_time = now.strftime("%H_%M_%S")
        default_col=df.columns
        i=0
        ##Rename column
        for col_name in schema:
            df=df.withColumnRenamed(default_col[i],col_name)
            i=i+1
        ##Cast Column
        df=df.select(list(map(lambda c:col(c).cast(schema[c]),df.columns)))
        df=df.withColumn('batch_date',lit(batch_date))
        df=df.withColumn('batch_time',lit( batch_time))
        df.printSchema()
        global record_count 
        batch_count=df.count()
        record_count=record_count+batch_count
        print("batch_date:",batch_date,"batch_time:",batch_time,"Current Records:",batch_count,"Total Records:",record_count)
        if outPath!=None:
            df.coalesce(1).write.partitionBy(['batch_date','batch_time'])\
            .mode(mode).format(format).save(outPath)
        else:
            df.show()
        if show and outPath!=None:
            df.show()        
    else:
        print("Empty Batch")

def streamToTbl(spark=None,batch_wait=10,host="127.0.0.1",port=-1,schema=None,outPath=None,mode="overwrite",format="csv",show=True):
    sc=spark.sparkContext
    ssc = StreamingContext(sc, batch_wait)
    sc.setLogLevel("ERROR")
    lines = ssc.socketTextStream(host,port)
    words = lines.flatMap(lambda x:x.split("|")).map(lambda x:x.split(",")).map(lambda x:[i.split("=")[1] for i in x])
    words.foreachRDD(lambda rdd: parseStream(rdd, spark,schema=schema,outPath=outPath,mode=mode,format=format,show=show))
    ssc.start()
    ssc.awaitTermination()




'''
spark=SparkSession.builder.appName("ReadStreamData").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
host= "127.0.0.1"
port=9999
#streamToTbl(spark=spark,host=host,port=port,schema=connectionSchema,outPath="file:///E:/Devesh/AccessDeck/streamedData/tbl1")

connectionSchema={
"access_deck_id":StringType(),
"cloud_provider_id":StringType(),
"cloud_provider_name":StringType(),
"record_ts":TimestampType(),
"status":IntegerType()
}
streamToTbl(spark=spark,host=host,port=port,schema=connectionSchema)

'''