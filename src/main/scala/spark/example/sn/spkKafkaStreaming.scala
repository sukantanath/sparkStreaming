package spark.example.sn

import org.apache.log4j.Logger
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{expr, from_json, struct}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object spkKafkaStreaming {
  @transient lazy val logger = Logger.getLogger(getClass.getCanonicalName)

  def main(args: Array[String]) = {

    //create spark session
    val ss = SparkSession.builder().master("local[12]")
      .appName("Spark streaming with kafka demo")
      .config("spark.streaming.stopGracefuullyonShutdown","true")
      .getOrCreate()

    import ss.implicits._

    //define incoming invoice json schema
    val schema = StructType(
      List(
        StructField("InvoiceNumber",StringType),
        StructField("CreatedTime",LongType),
        StructField("StoreID",StringType),
        StructField("PosID",StringType),
        StructField("CashierID",StringType),
        StructField("CustomerType",StringType),
        StructField("CustomerCardNo",StringType),
        StructField("TotalAmount",DoubleType),
        StructField("NumberOfItems",IntegerType),
        StructField("PaymentMethod",StringType),
        StructField("TaxableAmount",DoubleType),
        StructField("CGST",DoubleType),
        StructField("SGST",DoubleType),
        StructField("CESS",DoubleType),
        StructField("DeliveryType",StringType),
        StructField("InvoiceLineItems", ArrayType(
            StructType(
              List(
                StructField("ItemCode",StringType),
                StructField("ItemDescription",StringType),
                StructField("ItemPrice",StringType),
                StructField("ItemQty",StringType),
                StructField("TotalValue",StringType)
                )
              )
            ))
          )
        )

    //start read stream
    val kafkaDF = ss.readStream  //or use read for batch processing
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","invoices")
      .option("startingOffsets","earliest")
      .load()

    logger.info("Incoming data schema")
    kafkaDF.printSchema()

    //extract data from value column
    val valueDF = kafkaDF.withColumn("invoiceVal",from_json($"value".cast("string"),schema))
      .drop("value","timestampType")

    logger.info("invoice value schema")
    valueDF.printSchema()

    val explodedDF = valueDF.selectExpr("key","topic","partition","offset","timestamp","invoiceVal.InvoiceNumber",
      "invoiceVal.CreatedTime","invoiceVal.StoreID","invoiceVal.PosID","invoiceVal.CashierID","invoiceVal.CustomerType",
      "invoiceVal.CustomerCardNo","invoiceVal.TotalAmount","invoiceVal.NumberOfItems","invoiceVal.PaymentMethod",
      "invoiceVal.TaxableAmount","invoiceVal.CGST","invoiceVal.SGST","invoiceVal.CESS","invoiceVal.DeliveryType",
      "explode(invoiceVal.InvoiceLineItems) as LineItems")

    logger.info("exploded schema")
    explodedDF.printSchema()

    val invoiceDF = explodedDF.withColumn("ItemCode",expr("LineItems.ItemCode"))
      .withColumn("ItemDescription",expr("LineItems.ItemDescription"))
      .withColumn("ItemPrice",expr("LineItems.ItemPrice"))
      .withColumn("ItemQty",expr("LineItems.ItemQty"))
      .withColumn("TotalValue",expr("LineItems.TotalValue"))
      .drop("LineItems")

    //trigger notification
    val kafkaSinkWriterQuery = sendNotification(invoiceDF)

    //optional - writing flattened invoice records to file location
/*    val invWriterQuery = invoiceDF.writeStream.format("json")
      .queryName("Kafka invoice writter")
      .outputMode("append")
      .option("path","invoiceOP")
      .option("checkpointLocation","chk-point-dir1/invoice-writer")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()*/

    //invWriterQuery.awaitTermination()
    ss.streams.awaitAnyTermination() //for multiple query here invWriter & kafkaSinkWriterQuery use awaitAnyTermination
  }

  def sendNotification(df:DataFrame) = {
    val notificationDF = df.select("InvoiceNumber","CustomerCardNo","TotalAmount")
      .withColumn("EarnedLoyaltyPoints",expr("TotalAmount*0.3"))

    //change df to key,value format for sending to kafka
    val kafkaDF = notificationDF.selectExpr("InvoiceNumber as key",
      "to_json(named_struct('CustomerCardNo',CustomerCardNo,'TotalAmount',TotalAmount,'EarnedLoyaltyPoints',EarnedLoyaltyPoints)) as value")

    val kafDfAVro = df.select(expr("InvoiceNumber as key"),
      to_avro(struct("InvoiceNumber","CreatedTime","StoreID","PosID",
      "CustomerType","CustomerCardNo","TotalAmount","NumberOfItems","PaymentMethod",
      "TaxableAmount","DeliveryType","ItemCode","ItemDescription","ItemPrice","ItemQty","TotalValue")).as("value"))

    logger.info("avro schema sent by avro sink")
    kafDfAVro.printSchema()
    //kafkaDF.show(false)

   /* logger.info("Starting sending notifications to kafka")
      kafkaDF.writeStream
      .format("kafka")
      .queryName("Notification Writer")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","notifications")
      .outputMode("append")
      .option("checkpointLocation","chk-point-dir1/notify")
      .start()*/

    logger.info("Starting sending notifications to kafka with avro sink")
    kafDfAVro.writeStream
      .format("kafka")
      .queryName("Notification Writer in avro")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","invoice-items")
      .outputMode("append")
      .option("checkpointLocation","chk-point-dir1/invoice-reward")
      .start()
  }
}
