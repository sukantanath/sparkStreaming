package spark.example.sn

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object snTumblingWindow {

  @transient val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {
    val ss = SparkSession.builder.
      appName("Spark Streaming Tumbling Window").
      master("local[7]").
      config("spark.streaming.stopGracefullyOnShutDown","true").
      config("spark.sql.shuffle.partitions",3).getOrCreate()

    val stockSchema = StructType(List(
      StructField("CreatedTime",StringType),
      StructField("Type",StringType),
      StructField("Amount",IntegerType),
      StructField("BrokerCode",StringType)
    ))

    val kafkaSourceDF = ss.readStream.format("kafka").
      option("kafka.bootstrap.servers","localhost:9092").
      option("subscribe","trades").
      option("startingOffsets","earliest").load()

    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"),stockSchema).as("value"))
    valueDF.printSchema()

    val tradeDF = valueDF.select("value.*").
      withColumn("CreatedTime",to_timestamp(col("CreatedTime"),"yyyy-MM-dd hh:mm:ss")).
      withColumn("Buy",expr("case when Type == 'BUY' then Amount else 0 end")).
      withColumn("Sell",expr("case when Type == 'SELL' then Amount else 0 end"))

    val windowAggDF = tradeDF.withWatermark("CreatedTime","30 minute"). //watermark should be called before groupby with same event time column
      groupBy(window(col("CreatedTime"),"15 minute"))
        .agg(sum("Buy").as("TotalBuy"),sum("Sell").as("TotalSell"))

    val outputDF = windowAggDF.select("window.start","window.end","TotalBuy","TotalSell")

/* windowing other than timebased is not allowed in spark streaming as of now , can be done only in batch
   val runningWindow = Window.orderBy("end").rowsBetween(Window.unboundedPreceding,Window.currentRow)

  val finalOPdf = outputDF.withColumn("RTotalBuy",sum("TotalBuy").over(runningWindow))
      .withColumn("RTotalSell",sum("TotalSell").over(runningWindow))
      .withColumn("NetValue",expr("RTotalBuy - RTotalSell"))

    finalOPdf.show(false)*/

    windowAggDF.printSchema()

    val windowQuery = outputDF.writeStream.format("console").outputMode("complete").
      option("checkpointLocation","chk-point-dir1/tumbWindow").
      trigger(Trigger.ProcessingTime("1 minute")).start()

    logger.info("Starting aggregating buy & sell")
    windowQuery.awaitTermination()

  }
}
