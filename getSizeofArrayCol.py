from pyspark.sql.types import *
from pyspark.sql.functions import size

lstdata= [([4,5,6],),([1,2],),([7,8,9,0],)]

schema = StructType([
  StructField("data", ArrayType(IntegerType()), True)
])

df = spark.createDataFrame(lstdata, schema)
df.show(truncate = False)

df = df.withColumn("size", size("data"))
df.show(truncate = False)
