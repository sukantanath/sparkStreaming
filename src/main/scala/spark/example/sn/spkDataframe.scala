package spark.example.sn

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, explode, from_json, json_tuple}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object spkDataframe extends App {

  //*-------------------------SPARK SESSION-------------------------*//
  val ss: SparkSession = SparkSession.builder()
 //enableHiveSupport()
 .master("local[1]").appName("SparkByExamples.com")
 .getOrCreate()

  ss.sparkContext.setLogLevel("ERROR") //set loglevel

  import ss.implicits._

  //sample data
  val columns = Seq("language", "users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

  val snRdd = ss.sparkContext.parallelize(data) //rdd

  //*--------------------------Method-1 from RDD using toDF-------------*//

  //val snDF1 = snRdd.toDF //with default column name
  val snDF1 = snRdd.toDF("Language", "User") //with user provided column name
  //snDF1.show(false)
  //snDF1.printSchema()

  //*---------------------------Method-2 from RDD using createDataFrame-------------*//
  val snDF2 = ss.createDataFrame(snRdd).toDF(columns: _*)
  //snDF2.show(false)

  //*---------------------------Method-3 from Row[RDD]----------------------------*//
  val rowdata = Seq(Row("Java", "20000"),
 Row("Python", "100000"),
 Row("Scala", "3000"))

  val schema = StructType(Array(
 StructField("language", StringType, true),
 StructField("language", StringType, true)
  ))
  val rowRDD = snRdd.map(attributes => Row(attributes._1, attributes._2))
  var snDF3 = ss.createDataFrame(rowRDD, schema)

  //*---------------------------Method-4 from Seq------------------------------*//
  val snDF4 = data.toDF()
  //snDF4.show(false)

  //To be implemented--
  //from csv, json, xml, txt, hive"


  //*------------------------FROM DB---------------------------------------*//
  /* val df_mysql = ss.read.format("jdbc")
  .option("url", "jdbc:mysql://localhost:port/db")
  .option("driver", "com.mysql.jdbc.Driver")
  .option("dbtable", "tablename")
  .option("user", "user")
  .option("password", "password")
  .load()*/

  //anotherway for postgre
  /*val df_pgsql =ss.read.format("jdbc")
 .options(Map("dbtable" -> "sourceSchemaTable", "url" -> "url",
 "driver" -> "org.postgresql.Driver"))
 .load()*/

  //----------------------------- SELECT METHOD -----------------------------
  val selectdata = Seq(("James", "Smith", "USA", "CA"),
 ("Michael", "Rose", "USA", "NY"),
 ("Robert", "Williams", "USA", "CA"),
 ("Maria", "Jones", "USA", "FL")
  )
  val columnList = Seq("firstname", "lastname", "country", "state")

  import ss.implicits._

  val select_df = selectdata.toDF(columnList: _*)
  select_df.cache()
  println("Select df")
  select_df.show(false)
  Thread.sleep(100)

  val rdd1 = ss.sparkContext.parallelize(Seq(("chr1", 10016 ), ("chr1", 10017), ("chr1", 10018)))
  val rdd2 = ss.sparkContext.parallelize(Seq(("chr1", 10000, 20000), ("chr1",20000, 30000)))

  //join, filter
  rdd1.toDF("name","val").join(rdd2.toDF("name","min","max"),Seq("name"))
 .withColumn("minDev",col("val")-col("min"))
 .withColumn("maxDev",col("max")-col("val"))
 //.filter(col("val").between(col("min"),col("max")))
 .filter(col("minDev") >= 0 && col("maxDev") >= 0) //another way
 .groupBy(col("name")).count()

 .show(10)

  //json
  val jsonStr = """{'inpType':'stream','sla(in min)':1,'size(in gb)':2}"""
  val data1 = Seq((1,jsonStr))
  val jsonDF = data1.toDF("id","values")
  jsonDF.printSchema()
  jsonDF.show()
  jsonDF.withColumn("values",from_json(col("values"),MapType(StringType,StringType))).printSchema()
  val jsonDF1 = jsonDF.select(col("id"),json_tuple(col("values"),"inpType","sla(in min)","size(in gb)"))
 .toDF("id","inpType","sla","size")
  jsonDF1.show()

  //flatMap
  val arrayStructureData = Seq(
 Row("James,,Smith",List("Java","Scala","C++"),"CA"),
 Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
 Row("Robert,,Williams",List("CSharp","VB","R"),"NV")
  )

  val arrayStructureSchema = new StructType()
 .add("name",StringType)
 .add("languagesAtSchool", ArrayType(StringType))
 .add("currentState", StringType)

  val studentsDF = ss.createDataFrame(ss.sparkContext.parallelize((arrayStructureData)),arrayStructureSchema)
  implicit val encoder = RowEncoder(arrayStructureSchema)
  studentsDF.show(false)
  println("Flatmap")
  studentsDF.flatMap(r => {
 val lang = r.getSeq[String](1)
 lang.map((r.getString(0),_,r.getString(2)))
  }).toDF("name","language","state").show(false)

  println("explode")
  studentsDF.select(col("name"),explode(col("languagesAtSchool")),col("currentState")).show(false)

  //
  val partCfgRdd = ss.sparkContext.parallelize(Seq(("part1","cfg1"),("part2","cfg1"),("part3","cfg1"),("part4","cfg2"),("part5","cfg2")))
  val partPlatformRdd = ss.sparkContext.parallelize(Seq(("part1","cfg1","p1"),("part1","cg1","p2"),("part2","cfg1","p2")))

  val partCfgDF = partCfgRdd.toDF("partName","cfgName")
  val partPlatformDF = partPlatformRdd.toDF("partName","cfgName","platform")
  val joinedDF_c2 = partCfgDF.join(partPlatformDF,Seq("cfgName")).
 select(partCfgDF("*"),partPlatformDF("platform"))
  println("C2 data")
  joinedDF_c2.show(false)


  // DM
  val autoDev = Seq(("EMFP","PH5838P","ESG_HDD_SAS12G_ISE_512E_10TB_7_2K_3_5","EMEA Fulfillment Centers","OAK BLUFFS 740XD","382","HDD - ENTERPRISE","HV5CH","YF87J","bbc8e031-d712-4379-a4b2-2b55","PEI_JUN_CHNG ","PH0A87B","EMEA"),
("EMFP","PH5838P","ESG_HDD_SAS12G_ISE_512E_10TB_7_2K_3_5","EMEA Fulfillment Centers","OAK BLUFFS 740XD","382","HDD - ENTERPRISE","HV5CH","07FPR","bbc8e031-d712-4379-a4b2-2b55","PEI_JUN_CHNG ","PH0A87B","EMEA"),
("EMFP","PH5838P","ESG_HDD_SAS12G_ISE_512E_10TB_7_2K_3_5","EMEA Fulfillment Centers","OAK BLUFFS 740XD","382","HDD - ENTERPRISE","HV5CH","14YYC","bbc8e031-d712-4379-a4b2-2b55","PEI_JUN_CHNG ","PH0A87B","EMEA"),
("EMFP","PH5838P","ESG_HDD_SAS12G_ISE_512E_16TB_7_2K_3_5","EMEA Fulfillment Centers","OAK BLUFFS 740XD","382","HDD - ENTERPRISE","24HF9","CNXPV","bbc8e031-d712-4379-a4b2-2b55","PEI_JUN_CHNG ","PH0A87B","EMEA"),
("EMFP","PH5838P","ESG_HDD_SAS12G_ISE_512E_16TB_7_2K_3_5","EMEA Fulfillment Centers","OAK BLUFFS 740XD","382","HDD - ENTERPRISE","8MG73","CNXPV","bbc8e031-d712-4379-a4b2-2b55","PEI_JUN_CHNG ","PH0A87B","EMEA"),
("EMFP","PH5838P","ESG_HDD_SAS12G_ISE_512E_16TB_7_2K_3_5","EMEA Fulfillment Centers","OAK BLUFFS 740XD","382","HDD - ENTERPRISE","CNXPV","4N7V0","bbc8e031-d712-4379-a4b2-2b55","PEI_JUN_CHNG ","PH0A87B","EMEA"),
("EMFP","PH5838P","ESG_HDD_SAS12G_ISE_512E_16TB_7_2K_3_5","EMEA Fulfillment Centers","OAK BLUFFS 740XD","382","HDD - ENTERPRISE","CNXPV","VF206","bbc8e031-d712-4379-a4b2-2b55","PEI_JUN_CHNG ","PH0A87B","EMEA"),
("EMFP","PH5838P","ESG_HDD_SAS12G_ISE_4KN_8TB_7_2K_3_5","EMEA Fulfillment Centers","OAK BLUFFS 740XD","382","HDD - ENTERPRISE","DKGYV","CDDMJ","bbc8e031-d712-4379-a4b2-2b55","PEI_JUN_CHNG ","PH0A87B","EMEA"),
("EMFP","PH5838P","ESG_HDD_SAS12G_ISE_4KN_8TB_7_2K_3_5","EMEA Fulfillment Centers","OAK BLUFFS 740XD","382","HDD - ENTERPRISE","DKGYV","XX7MT","bbc8e031-d712-4379-a4b2-2b55","PEI_JUN_CHNG ","PH0A87B","EMEA"),
("EMFP","PH5838P","ESG_HDD_SAS12G_ISE_4KN_8TB_7_2K_3_5","EMEA Fulfillment Centers","OAK BLUFFS 740XD","382","HDD - ENTERPRISE","DKGYV","05WTN","bbc8e031-d712-4379-a4b2-2b55","PEI_JUN_CHNG ","PH0A87B","EMEA"),
("EMFP","PH5838P","ESG_HDD_SAS12G_ISE_4KN_8TB_7_2K_3_5","EMEA Fulfillment Centers","OAK BLUFFS 740XD","382","HDD - ENTERPRISE","DKGYV","K6646","bbc8e031-d712-4379-a4b2-2b55","PEI_JUN_CHNG ","PH0A87B","EMEA"),
("EMFP","PH5838P","SSD_ESG_2_5_960GB_SATA_RI_AGNOSTIC_6Gbs","EMEA Fulfillment Centers","OAK BLUFFS 740XD","1562 ","SSD - ENTERPRISE","V36D9","6KCYT","bbc8e031-d712-4379-a4b2-2b55","YONG_XION_OOI","PH0A87B","EMEA"))

  val autoDevCols = Seq("facility","pltfrm_nbr","cfg","region","pltfrm_nm","commodity_id","commodity","part_to","part_from","seqid","user_name","lob","regions_affected")

  val autoDevDF = autoDev.toDF(autoDevCols: _*)

  println("Auto Deviation DF...")
  autoDevDF.show(false)

  val autoDevDF1 = autoDevDF.select(col("pltfrm_nbr"),col("cfg"),col("commodity"),
                                    col("part_to"),col("part_from"),col("lob")).
    groupBy("pltfrm_nbr","cfg","part_to").agg(count("part_from").alias("dev_part_count"))

  println("Autodev df1")
  autoDevDF1.show(false)

  val devtnCountDF = autoDevDF.alias("df1").join(autoDevDF1.alias("df2"),
    col("df1.pltfrm_nbr") === col("df2.pltfrm_nbr") &&
    col("df1.cfg") === col("df2.cfg") &&
    col("df1.part_to") === col("df2.part_to"),"inner")
    .select("df1.*","df2.dev_part_count").orderBy(desc("dev_part_count"))

  //devtnCountDF.

}
