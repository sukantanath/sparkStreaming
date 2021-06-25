package akka.example.sn

import scala.annotation.tailrec
import scala.collection.mutable

object scalaBasicsRevisited extends App{

  //VAL vs VAR
  val aCond:Boolean = false
  //aCond = true -- wont work , immutable

  var aVar = 13 //type inference
  aVar = 15

  //expression
  val aCondVal = if(aCond) aVar else 0

  //code block
  val aCodeblockVal = {
    if (!aCond) aVar + 2
    else println("not maching any value"); 0
  }

  //unit type
  val unitVal = println("No type")

  //function
  def firstFunc(x: Int): Int = x*2

  @tailrec
  def factorial(num:Int,accu:Int):Int = {
    if (num <= 0) accu
    else factorial(num-1,accu*num)
  }

  //OOP
  class Project
  class FeatureTeam extends Project
  val supplyChainBacklog: FeatureTeam = new FeatureTeam

  //trait

  //WrapperdArray
  println("Wrapped Array....")
  val wrpArr = mutable.WrappedArray.make(Array(1,2,3,4,5,6,4,1,3,2))
  val a1: Seq[String] = mutable.WrappedArray.make(Array("A","B","C","A","D"))

  val res = wrpArr.toSet.subsets(2).asInstanceOf[Iterator[Set[Int]]].map(_.toList).toList
  println(res)

  val res1 = a1.toSet.sliding(2).toList
  println(res1)
  
  val res2 = a1.toArray.mkString("")
  println(res2)

  val res3 = a1.grouped(2).toArray.map(_.toArray.mkString(""))
  println(res3)

}
