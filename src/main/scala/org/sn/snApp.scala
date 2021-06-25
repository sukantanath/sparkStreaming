package org.sn

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import scala.annotation.tailrec
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.ArrayBuffer
import util.control.Breaks.{breakable,break}

case class MyCC(name:String, age: String = null, section: String = "")
case class ObjSalesOrderLine(
                              soLineRef : String,
                              mfgMethod: String,
                              mfgLob : String
                            )

case class c1(part: String, consump_fac: String, desc: String)
case class c2(consump_fac: String, mas_loc: String)

/**
 * Hello world!
 *
 */
object snApp extends App{
  println( "Hello World!" )
  var respSO:java.util.ArrayList[ObjSalesOrderLine] = new java.util.ArrayList[ObjSalesOrderLine]()
  respSO.add(ObjSalesOrderLine("43830500","STORAGE_DELLEMC",null))
  respSO.add(ObjSalesOrderLine("43830510","STORAGE_SW",null))
  respSO.add(ObjSalesOrderLine("43830520","STORAGE_SW",null))
  respSO.add(ObjSalesOrderLine("43830530","STORAGE_SW",null))
  respSO.add(ObjSalesOrderLine("43830540","STORAGE_",null))
  respSO.add(ObjSalesOrderLine("43830550","STORAGE_",null))
  println(respSO)

  val lineCount = respSO.map(rec => (rec.soLineRef -> rec.mfgMethod)).toMap.groupBy(_._2).mapValues(_.size)
  println(lineCount.filter(rec => rec._1 == "STORAGE_DELL" || rec._1 == "STORAGE_DELLEMC").isEmpty)
      println("111 ")
  val ab = ArrayBuffer(ObjSalesOrderLine("43830500","STORAGE_DELLEMC",null),
    ObjSalesOrderLine("43830501","STORAGE_DELL",null),
    ObjSalesOrderLine("43830502","STORAGE_DELLEMC",null))
  val abs1 = ab.map(_.soLineRef).mkString(",")
  println(abs1)
  val gson = (new GsonBuilder()).setPrettyPrinting().create()
  println("JSON usng gson")
  //println(gson.toJson(ab))
  implicit val formats = DefaultFormats
  val jsonString = write(ab)
  println(jsonString)

  val part = ArrayBuffer(c1("p3","c3","p3desc"),
    c1("p4","c4","p4desc"),
    c1("p2","c2","p2desc"),
    c1("p1","c1","p1desc"))

  val masloc = ArrayBuffer(c2("c1","m1"),c2("c2","m2"))

  val resultSet = ArrayBuffer(c1("","",""))

/*    part.foreach(prec => {
      //println(prec.part,prec.consump_fac)
      masloc.foreach(rec =>
          if (prec.consump_fac == rec.consump_fac)
            resultSet += c1(prec.part, rec.mas_loc, prec.desc))
        }
      )

  part.foreach(rec => {
    if(resultSet.filter(c => c.part == rec.part).isEmpty) resultSet += rec
  })*/

/*  part.foreach(prec => {
    var mLoc = ""
    if(masloc.filter(rec => rec.consump_fac == prec.consump_fac).nonEmpty)
      mLoc = masloc.filter(rec => rec.consump_fac == prec.consump_fac)(0).mas_loc
    println(mLoc)
    if (mLoc.nonEmpty) resultSet += c1(prec.part,mLoc,prec.desc)
    else resultSet += c1(prec.part,prec.consump_fac,prec.desc)
  })*/

  part.sortBy(p => p.consump_fac).
    map(p => {
      if (masloc.filter(m => m.consump_fac == p.consump_fac).nonEmpty)
        resultSet += c1(p.part,masloc.filter(m => m.consump_fac == p.consump_fac)(0).mas_loc,p.desc)
      else
        resultSet += c1(p.part,p.consump_fac,p.desc)
    })


  resultSet.filter(c => c.part.nonEmpty).foreach(println)


  println("112")
      /*[ObjSalesOrderLine("43830500","STORAGE_DELLEMC",null),
      ObjSalesOrderLine("43830510","STORAGE_SW",null),
      ObjSalesOrderLine("43830520","STORAGE_SW",null),
      ObjSalesOrderLine("43830530","STORAGE_SW",null),
      ObjSalesOrderLine("43830540","STORAGE_DELLEMC",null),
      ObjSalesOrderLine("43830550","STORAGE_DELLEMC",null)]*/
      var ccobj1 = MyCC(name="Sukanta")
      //Instantiate variable without value, pass value later
      var myVar1:Int = _
      println("Variable Value before instantiate:",myVar1)
      myVar1 = 7
      println("Variable Value ",myVar1)

      //string interpolation
      val day = "Monday"
      println(s"Today is $day")

      //string interpolation with case class
      case class employee(name:String,id:Int,department:String)
      val emp1 = employee("Sukanta",1211947,"DSC")
      println(s"Employee 1 details are ,name = ${emp1.name}, id = ${emp1.id}")
      println(s"Checking if employee is in DSC = ${emp1.department == "DSC"}")

      //string interpolation with f
      val temp = 29.4
      println(s"Today's temparature $temp")
      println(f"Or it can be expressed as $temp%.2f")

      //raw interpolation
      println(s"Once again \t$temp")
      println(raw"Nothing to transform \t$temp")

      //escape characters
      val msg1 = "{\"templateId\":1,\"templatName\":\"SampleTemplate\"}"
      println("Value of message1 with \\ is ",msg1)

      val msg2 = """{"fileId":2,"fileName":"Uploaded_file.csv","timeStamp":"17/02/2021 8:55AM"}"""
      println("Value of message2 with \"\"\" is ",msg2)

      val msg3 =
        """
          |{"ruleId":3,
          |"ruleName":"ruleset1",
          |"ruleManager":"Sukanta"}""".stripMargin

      println("Value of message3 with stripmargin is ",msg3)

      //if else
      val x = 3;val y = 12

      if(x >= 3)
        if (y < 10)
          println("X and Y both are good")
        else if(y == 10)
          println("Y is borderline")
        else
          println("Y is bad")

      val z = if(x <= 5 & y > 1) (x*y) else 1
      println("value of z ",z)

      //for loop
      println("Simple for")
      for( ff <- 1 to 10) {
        println(s"Even number:${if(ff%2 == 0) ff else ""}")
      }

      println("For with if within loop")
      for( fg <- 1 to 100 if(fg % 10 == 0)){println("10's table:",fg)}

      println("For with yield")
      val nameList = List("SN","MB","MN","DH","RR","PB","SM","PB","RM","DK","PS")

      //tail recursion
      //concat a string n-times
      @tailrec
      def concatStr(inpStr:String,n:Int,accu:String): String =
        if (n <= 0 ) accu else concatStr(inpStr,n-1,accu+inpStr)

      println("Concat String using tail-recursion: ",concatStr("SN",3,""))

      //check if a number is prime
      def isPrime(num:Int): Boolean = {

        def isPrimeHelper(t:Int): Boolean =
          if (t <= 1) true
          else ((num % t != 0) && isPrimeHelper(t-1))

        isPrimeHelper(num/2)
      }

      println(isPrime(36))

      //prime with tail recursion
      def isPrime1(num:Int) : Boolean ={
        def isPrimeTailRec(t:Int,isTillBoolean: Boolean): Boolean = {
          if (!isTillBoolean) false
          else if (t <= 1) true
          else isPrimeTailRec(t-1, num % t != 0 && isTillBoolean)
        }

        isPrimeTailRec(num/2,true)
      }

      def fibbonaciFunc(num:Int) = {
        def fibbonaciRec(i:Int, last:Int, secondLast:Int):Int =
          if (i >= num) last
          else fibbonaciRec(i+1,last+secondLast,last)

        if (num < 2) 1
        else fibbonaciRec(2,1,1)
      }

      println("Fibbonaci number for 8 is: ",fibbonaciFunc(8))

      val l1 = 'A'
      val l2 = 'Z'
      println(l1 + "to" + l2)

      val list1 = List(1,2)
      println(-1 +: list1 :+ 0)
  }



