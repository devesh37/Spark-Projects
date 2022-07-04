#CustomSchema while reading any file types
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,StringType,FloatType,StructField


'''
df.write.format('jdbc').options(
      url='jdbc:mysql://localhost/photo_app',
      driver='com.mysql.cj.jdbc.Driver',
      dbtable='tempr',
      user='devesh',
      password='root').mode('overwrite').save()
'''

mysql_props={
'url': 'jdbc:mysql://localhost/photo_app',
'user': 'devesh',
'password': 'root',
'driver':'com.mysql.cj.jdbc.Driver'
}


def dfToMysql(spark=None,s3=None,mode="overwrite",tgt_tabl=None,jdbc_props=mysql_props):
      df=spark.read.parquet(s3)
      df=df.select([c for c in df.columns if c not in {'record_ts'}])
      df.write.format('jdbc').options(**jdbc_props).option("dbtable",tgt_tabl).mode(mode).save()

spark=SparkSession.builder.appName("DataframeReadExample").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")



dfToMysql(spark=spark,s3="file:///E:/Devesh/AccessDeck/data/jrnl/connection",mode="overwrite",tgt_tabl="connection_data",jdbc_props=mysql_props)

dfToMysql(spark=spark,s3="file:///E:/Devesh/AccessDeck/data/jrnl/datatransfer",mode="overwrite",tgt_tabl="datatransfer_log",jdbc_props=mysql_props)

dfToMysql(spark=spark,s3="file:///E:/Devesh/AccessDeck/data/jrnl/repo",mode="overwrite",tgt_tabl="repo",jdbc_props=mysql_props)
