"""hdfs -format namenode
hdfs dfsadmin -report
jps
spark-shell
"""




S3_INPUT_FILE = "s3://group8bucket/train.csv"
S3_OUTPUT_FOLDER = "s3://group8bucket/output/"

"""Khai bao thu vien SaveMode Append (overwrite)"""
import org.apache.spark.sql.SaveMode

"""khai bao bien df """
val df = spark.read.format("csv").option("header", true).load("s3://group8bucket/train.csv")
"""Tuong tac voi file csv"""
df.count()
df.show(50)

"""Ghi file parquet"""
""""""
df.select("*").write.mode(SaveMode.Append).format("parquet").save("s3://group8bucket/output/")

"""Doc file parquet"""
"""Bien moi trương"""
val sqlContext = new org.apache.spark.sql.SQLContext(sc)


val dftrain = sqlContext.read.parquet("s3://group8bucket/output/")
"""Tuong tac voi parquet"""
df.count()
df.show()

dftrain.createOrReplaceTempView("ParquetTable")//Tao duoc view

spark.sql("SELECT * FROM ParquetTable where user_answer >= 2").show()

