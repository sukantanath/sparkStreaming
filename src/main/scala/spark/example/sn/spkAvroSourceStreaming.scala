package spark.example.sn

import java.nio.file.{Files, Paths}

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions.from_avro
object spkAvroSourceStreaming {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {
    val ss = SparkSession.builder.
      appName("Spark streaming with avro source").
      master("local[3]").
      config("spark.streaming.stopGracefullyOnSHutdown","true").
      getOrCreate()

    val kafkaSourceDF = ss.readStream.
      format("kafka").
      option("kafka.bootstrap.servers","localhost:9092").
      option("subscribe","invoice-items").
      option("startingOffsets","earliest").
      option("mode", "PERMISSIVE").
      load()

    //kafkaSourceDF.show()

    val avroSchema = new String(Files.readAllBytes(Paths.get("avroSchema/invoice-items")))
    val valueDF = kafkaSourceDF.select(from_avro(col("value"),avroSchema).as("value"))

    //valueDF.show(false)
    valueDF.printSchema()

    val rewardsDF = valueDF.filter("value.CustomerType == 'PRIME'")
      .groupBy("value.CustomerCardNo")
      .agg( sum("value.TotalValue").as("TotalPurchase"),
        sum(expr("value.TotalValue *0.3").cast("integer")).alias("TotalRewards"))

    //rewardsDF.show(false)

    val kafkaTrgetDF = rewardsDF.select(expr("CustomerCardNo as key"),
      to_json(struct("TotalPurchase","totalRewards")).as("value"))

    kafkaTrgetDF.writeStream.format("kafka")
      .queryName("Kafka customer reward writer")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","customer-rewards")
      .option("checkpointLocation","chk-point-dir1/customer-reward")
      .outputMode("update")
      .start().awaitTermination()


  }
}
