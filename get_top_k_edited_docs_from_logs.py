from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = (
    SparkSession.builder.appName("TopKDocumentsJob")
    .config(
        "spark.jars",
        "/Users/mohit/My_Work/ArteriaAI/spark-3.5.0-bin-hadoop3/jars/sqlite-jdbc-3.44.1.0.jar",
    )
    .getOrCreate()
)

# SQLite connection properties
jdbc_url = "jdbc:sqlite:/Users/mohit/My_Work/ArteriaAI/Top_K_distributed_demo/GatewayService/db.sqlite3"
properties = {"driver": "org.sqlite.JDBC"}

# Read data from SQLite into a DataFrame
logger_edits = spark.read.jdbc(
    url=jdbc_url, table="gateway_edits", properties=properties
)

# Aggregate edits count for each document
# Group by document and count the number of edits
document_edits = logger_edits.groupBy("document").count()

# Order by the count in descending order
document_edits = document_edits.orderBy(col("count").desc())

# Set the value of k (number of documents with most edits you want)
k = 10

# Select the top k documents
top_k_documents = document_edits.limit(k)

# Show the result
top_k_documents.show()

# Stop the Spark session
spark.stop()
