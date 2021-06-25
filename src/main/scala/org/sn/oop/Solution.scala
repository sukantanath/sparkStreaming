package org.sn.oop

import scala.{:+, ::}

object Result {
  case class Employee (name: String, age: Int, address: String)

  /*
   * Complete the 'ageToEmployees' function below.
   *
   */

  //Map[Int, List[Employee]]
  def ageToEmployees (empList: List[Employee]) = {
    //empList.foreach(println)
    val age2EmpMap = Map(empList map{emp => (emp.age,List(emp))}: _*)

    //age2EmpMap.foreach(println)

    val y = empList.map(a => (a.age,List(a)))
    //y.foreach(println)
    println("SN")
/*    val yy = scala.collection.immutable.ListMap(y.sortBy(x => x._1) : _*)
    yy.foreach(println)*/
    val ppp = (1 -> List(Employee("SN",35,"a1")))
    val zz = y.sortBy(x => x._1)
    var res3 = Map[Int,List[Employee]]()
    //zz.foreach(println)
    val sd = empList.map(a => (a.age,List(a))).foldLeft(Map[Int,List[Employee]]())
            {case (state,(i,e)) =>println(s"In $state,${i.toString},$e" );state.updated(i,state.get(i).getOrElse(List[Employee]()) ::: e) }
    sd.foreach(println)

    /*


  import scala.collection.mutable.ListBuffer
  case class Employee (name: String, age: Int, address: String)
  object CaseClassExample {
  def main(args: Array[String]) = {
    val lst = List(
      Employee("s1", 1, "a1"),
      Employee("s2", 2, "a1"),
      Employee("s3", 2, "a1"),
      Employee("s4", 3, "a1"),
      Employee("s5", 3, "a1"),
      Employee("s6", 3, "a1"),
      Employee("s7", 4, "a1"),
      Employee("s8", 4, "a1"),
      Employee("s9", 4, "a1"),
      Employee("s10", 4, "a1"),
      Employee("s11", 5, "a1"),
      Employee("s12", 5, "a1"),
      Employee("s13", 5, "a1"),
      Employee("s14", 5, "a1"),
      Employee("s15", 5, "a1"),
      Employee("s16", 6, "a1"),
      Employee("s17", 6, "a1"),
      Employee("s18", 6, "a1"),
      Employee("s19", 6, "a1"),
      Employee("s20", 6, "a1"),
      Employee("s21", 6, "a1"),
      Employee("s22", 7, "a1"),
      Employee("s23", 7, "a1"),
      Employee("s24", 7, "a1")
    )
    var map1 = scala.collection.mutable.Map[Int, ListBuffer[Employee]]()
    lst.foreach(elem => {
      if(map1.contains(elem.age)) {
        val list = map1.get(elem.age).get.+=(elem)
        map1.put(elem.age, list)
      }
      else
        map1.put(elem.age,ListBuffer(elem))
    })
    map1.foreach(elem => println("elem : " + elem._1 + " -- " + elem._2))
    }
  }
     */
/*    zz.foreach{
      case (age,emp) =>
        res3 += (age -> res3.getOrElse(age,List[Employee]()).reduce(emp))
          //.map(rec => Employee(rec) ::: emp))
    }
    res3.foreach(println)*/
    println("flatmap")
  /*  val res3 = zz.foldLeft(Map[Int,List[Employee]]()){case (k,v) => (k,v::v)}
    res3.foreach(println)*/
    val a = Array("a", "c", "c", "z", "c", "b", "a")
    val ww = a.zipWithIndex.foldLeft(Map[String,Int]()){case (l,idx) => l }
    //ww.foreach(println)

    val x1 = a.zipWithIndex /*.foldRight(" ")
      {case ((e,i),m)=> (e + i + m)}*/
    //println(x1)
    println("...")
    val res1 = empList
    //res1.foreach(println)
    println("Final")
    val res2 = empList.groupBy(a => a.age)
    res2.foreach(println)

 /*   val res = y.foldLeft(""){
      case("",(k,v)) => k + "->" + v
    }
    println(res)
*/
      /*.flatMap(a => a.age.ma(ab => ab -> a.name))
      .groupBy(_._1)
      .map(g => g._1 -> g._2.map(_._2)).toMap
*/
  }

}

object Solution {
  def main(args: Array[String]) {
    import Result.Employee

    //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

    val lst = List(
      Employee("s1", 1, "a1"),
      Employee("s2", 2, "a1"),
      Employee("s3", 2, "a1"),
      Employee("s4", 3, "a1"),
      Employee("s5", 3, "a1"),
      Employee("s6", 3, "a1"),
      Employee("s7", 4, "a1"),
      Employee("s8", 4, "a1"),
      Employee("s9", 4, "a1"),
      Employee("s10", 4, "a1"),
      Employee("s11", 5, "a1"),
      Employee("s12", 5, "a1"),
      Employee("s13", 5, "a1"),
      Employee("s14", 5, "a1"),
      Employee("s15", 5, "a1"),
      Employee("s16", 6, "a1"),
      Employee("s17", 6, "a1"),
      Employee("s18", 6, "a1"),
      Employee("s19", 6, "a1"),
      Employee("s20", 6, "a1"),
      Employee("s21", 6, "a1"),
      Employee("s22", 7, "a1"),
      Employee("s23", 7, "a1"),
      Employee("s24", 7, "a1")
    )
    val d : Map[Int, List[Employee]]= Map(
      5 -> List(Employee("s11",5,"a1"),
        Employee("s12",5,"a1"),
        Employee("s13",5,"a1"),
        Employee("s14",5,"a1"),
        Employee("s15",5,"a1")),
      1 -> List(Employee("s1",1,"a1")),
      6 -> List(Employee("s16",6,"a1"),
        Employee("s17",6,"a1"),
        Employee("s18",6,"a1"),
        Employee("s19",6,"a1"),
        Employee("s20",6,"a1"),
        Employee("s21",6,"a1")),
      2 -> List(Employee("s2",2,"a1"),
        Employee("s3",2,"a1")),
      7 -> List(Employee("s22",7,"a1"),
        Employee("s23",7,"a1"),
        Employee("s24",7,"a1")),
      3 -> List(Employee("s4",3,"a1"),
        Employee("s5",3,"a1"),
        Employee("s6",3,"a1")),
      4 -> List(Employee("s7",4,"a1"),
        Employee("s8",4,"a1"),
        Employee("s9",4,"a1"),
        Employee("s10",4,"a1")))

    val result = Result.ageToEmployees(lst)

    def mapEqual(m1: Map[Int, List[Employee]], m2: Map[Int, List[Employee]]): Boolean = {

      def mapContentEql(m1: Map[Int, List[Employee]], m2: Map[Int, List[Employee]]): Boolean = {
        m1.foldLeft(true) { (acc, x) =>
          acc && {
            val y1 = (m2.getOrElse(x._1, List.empty[Employee])).sortBy(_.name)
            val y2 = x._2.sortBy(_.name)
            y1.equals(y2)
          }
        }
      }

      if ((m1.keySet == m2.keySet) && mapContentEql(m1,m2)) true else false
    }

    //assert(mapEqual(result, d))

    //printWriter.println(result)

    //printWriter.close()
  }
}