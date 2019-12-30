import findspark 
findspark.init() 

from pyspark.context import SparkContext 
from pyspark.sql.session import SparkSession   
sc = SparkContext.getOrCreate() 
spark = SparkSession(sc)

fireServiceCallsDF = spark.read.csv('s3://k-spark2/d1/696314371547-aws-billing-detailed-line-items-with-resources-and-tags-2019-12.csv', header=True, inferSchema=True)  
