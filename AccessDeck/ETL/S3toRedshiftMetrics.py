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

redshift_props={

'url': 'jdbc:redshift://access-db.cc6spbysfcw5.us-east-1.redshift.amazonaws.com:5439/dev',
'user': 'access-user',
'password': 'Access_123',
'driver':'com.amazon.redshift.jdbc42.Driver'
}


def dfToRedShift(spark=None,s3=None,mode="overwrite",tgt_tabl=None,redshift_props=redshift_props):
      df=spark.read.parquet(s3)
      df.write.format('jdbc').options(**redshift_props).option("dbtable",tgt_tabl).mode(mode).save()

spark=SparkSession.builder.appName("DataframeReadExample").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")



dfToRedShift(spark=spark,s3="file:///E:/Devesh/AccessDeck/data/jrnl/connection",mode="overwrite",tgt_tabl="connection_data",redshift_props=redshift_props)

dfToRedShift(spark=spark,s3="file:///E:/Devesh/AccessDeck/data/jrnl/datatransfer",mode="overwrite",tgt_tabl="datatransfer_log",redshift_props=redshift_props)

dfToRedShift(spark=spark,s3="file:///E:/Devesh/AccessDeck/data/jrnl/repo",mode="overwrite",tgt_tabl="repo",redshift_props=redshift_props)
