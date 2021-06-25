package org.sn.oop

object ScalaOOBasic extends App{
  val p1 = new Persons("SN",35)
  p1.greet("NS")

}

class Persons(name: String,val age: Int)//constructor - not class fields , can not be accessed
  {
    val org = "google"
    println("Bye bye!")

    def greet(name: String) = {
      println(s"${this.name} says Hello! to $name")
    }

  }