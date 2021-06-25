package spark.example.sn

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger

object spkStreaming {
  @transient lazy val logger = Logger.getLogger(getClass.getName) //not to serialize

  def main(args: Array[String]):Unit = {
    val ss = SparkSession.builder().master("local[12]").appName("Spark Streaming Demo")
      .config("spark.streaming.stopGracefullyOnShutdown","true") //for graceful shutdown
      .config("spark.sql.shuffle.partitions","20")
      .config("spark.sql.streaming.schemaInference","true") //by default streaming schema inference ins false
      .getOrCreate()

    //socket stream
    socketStreamProcessor(ss)

    //file stream
    //fileStreamProcessor(ss)
  }

  def socketStreamProcessor(ss:SparkSession) = {
    //lod stream data as input dataframe

    val linesDF = ss.readStream.format("socket") //to read from terminal
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    linesDF.printSchema()

    val wordsDF = linesDF.select(expr("explode(split(value,' ')) as word")) //separate words by space
    val countDF = wordsDF.groupBy("word").count() //output dataframe

    val wcQuery = countDF.writeStream.format("console") //sink is console
      .option("checkpointLocation", "chk-point-dir") //store stream progress info
      .outputMode("update")
      .start()

    wcQuery.explain()
    logger.info("Listening to localhost 9999")
    wcQuery.awaitTermination()

  }

  def fileStreamProcessor(ss:SparkSession) = {
    ss.conf.set("spark.sql.streaming.schemaInference", "true")
    val rawDF = ss.readStream.format("json"). //file format is json
      option("path", "SampleData").
      option("maxFilesPerTrigger", 1). //will pick up 1 file per batch
      option("cleanSource", "archive").
      option("sourceArchiveDir", "./archived/files").
      load()
    rawDF.printSchema()

    val convertedDF = rawDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID", "CustomerType", "PaymentMethod", "DeliveryType",
      "DeliveryAddress.City", "DeliveryAddress.State", "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")
    convertedDF.printSchema()

    val flattenedDF = convertedDF.withColumn("ItemCode",expr("LineItem.ItemCode"))
      .withColumn("ItemDescription",expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice",expr("LineItem.ItemPrice"))
      .withColumn("ItemQty",expr("LineItem.ItemQty"))
      .withColumn("TotalValue",expr("LineItem.TotalValue"))
      .drop("LineItem")


    val invoiceWriterQuery = flattenedDF.writeStream.format("json").
      option("path", "output"). //directory name is output
      option("checkpointLocation", "chk-point-dir").
      outputMode("append"). //append - insert, update - upsert, complete - overwrite
      trigger(Trigger.ProcessingTime("1 minute")). //minibatch to start in every 1 min
      queryName("Flattened Invoice Json Writer").
      start()

    logger.info("Flattened Invoice writer starting")
    invoiceWriterQuery.awaitTermination()
  }

  /* checkpoint stores:
    read-position - start and end position of input stream
    state information - intermediate state of application variables

    Exactly once delivery in case of restart (fault tolerance) is dependent on -
    1. restart with same checkpoint location - same checkpoint location as it was before steam stopped
    2. use a replayable source - source can resend old data, eg, kafka
    3. use deterministic computation - consumer application logic yields same result every time it gets same input data
    4. use and idempotence sink - sing can determine duplicate record
   */

}
