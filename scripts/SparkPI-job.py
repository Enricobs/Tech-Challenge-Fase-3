import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, trim


#Inicialização do Glue + Spark
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)




df = spark.read.parquet(
    "s3://lab-385615870279/data-output/gold/2025/"
)


df_processado = df.withColumn(
    "genero",
    trim(col("genero"))
)

#Mantém apenas idades válidas
df_processado = df_processado.filter(
    (col("idade") >= 18) &
    (col("idade") <= 100)
)



df_processado.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(
        "s3://lab-385615870279/data-output/spark/2025/"
    )



job.commit()