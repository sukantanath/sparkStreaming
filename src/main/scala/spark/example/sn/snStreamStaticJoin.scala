package spark.example.sn

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, to_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object snStreamStaticJoin {
  @transient val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {
    val ss = SparkSession.builder.
      appName("Spark Streaming Tumbling Window").
      master("local[7]").
    /*  config("spark.cassandra.connection.host","localdb"). //for local cassandra
      config("spark.cassandra.connection.port","9842"). //for local cassandra
      config("spark.sql.extensions","com.datastax.spark.connector.CassandraSparkExtensions"). //for local cassandra
      config("spark.sql.catalog.lh","com.datastax.spark.connector.datasoruce.CassandraCatalog"). //for local cassandra*/
      config("spark.streaming.stopGracefullyOnShutDown", "true").
      config("spark.sql.shuffle.partitions", 3).getOrCreate()

    val loginSchema = StructType(List(
      StructField("created_time",StringType),
      StructField("login_id",StringType)
    ))

    val kafkaSourceDF = ss.readStream.format("kafka").
      option("kafka.bootstrap.servers","localhost:9092").
      option("subscribe","logins").
      option("startingOffsets","earliest").load()

    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"),loginSchema).as("value"))
    val loginDF = valueDF.select("value.*").withColumn("created_time",to_timestamp(col("created_time"),"yyyy-MM-dd HH:mm:ss"))
    loginDF.printSchema()

    //read from cassandra
    /*val staticDF = ss.read.format("org.apache.sql.cassandra").
      option("keyspace","spark_db").
      option("table","users").load()
*/
    //if cassandra is not avaible use manually created df
    val staticSeq = Seq(("100001", "Prashant", "2019-02-05 10:05:00"),
                        ("100009", "Alisha", "2019-03-07 11:03:00"),
                        ("100087", "Abdul", "2019-06-12 09:43:00"),
                        ("100091", "New User", "2019-06-12 09:43:00"))

    val staticCols = Seq("login_id","user_name","last_login")

    import ss.implicits._
    val staticDF = staticSeq.toDF(staticCols: _*)

    val joinedDF = loginDF.join(staticDF,loginDF.col("login_id") === staticDF.col("login_id"),"inner").
              drop(loginDF.col("login_id"))

    val outputDF = joinedDF.select(col("login_id"),col("user_name"),col("created_time").as("last_login"))

    //write back to casandra
    /*val outputQuery = outputDF.writeStream.foreachBatch(writeToCassandra _).
      outputMode("update").
      option("checpointLocation","chk-point-dir1/staticjoin").
      trigger(Trigger.ProcessingTime("1 minute")).start()
*/
    //if cassandra not available write to console
    val outputQuery = outputDF.writeStream.format("console").outputMode("append").
      option("checkpointLocation","chk-point-dir1/staticjoin").
      trigger(Trigger.ProcessingTime("1 minute")).start()

    logger.info("Writing back to cassandra")
    outputQuery.awaitTermination()

  }

  def writeToCassandra(df:DataFrame,batchId: Long) = {

    df.write.format("org.apache.sql.cassandra").option("keyspace","spark_db").option("table","users").mode("append").save()

    df.show(false)
  }
}
