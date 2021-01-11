from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///logs").appName("StructuredStreamingDemo").getOrCreate()

