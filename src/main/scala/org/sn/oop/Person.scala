package org.sn.oop

class Person(val name: String,val age: Int) {
  def greet(name: String) = println(s"${this.name} saying hi to ${name}")
  def +(person: Person) = s"${person.name} is hanging out with ${this.name}"
  def +(nickName: String) = new Person(name+ " " +nickName,age)
  def unary_+ : Person = new Person(this.name, this.age+1)
  def learns(sub: String) = s"${this.name} learns $sub"
  def learnScala() = this learns "Scala"
  def apply() = println("This special method joins OOPS with Functional Programming")
  def apply(num: Int) = println(s"$name codes $num hours a day")
}

class Writer(firstName: String, surName:String, val age:Int) {
  def fullName() : String= this.firstName + " " + this.surName
}

class Novel(name: String, yearOfRelease: Int, author: Writer) {
  def authorAge() = yearOfRelease - author.age
  def isWrittenBy(author: Writer) = author ==  this.author
  def copy(newYearOfRelease: Int): Novel = new Novel(this.name,newYearOfRelease,this.author)
}

class Counter(num :Int) {
  def currCount = this.num
  def inc = new Counter(num + 1)
  def dec = new Counter(num - 1)

  def inc(step: Int) = new Counter(num +step)
  def dec(step: Int) = new Counter(num - step)
}
object runCode extends App {
  val p1 = new Person("Sukanta",35)
  p1.greet("Mithai")
  p1 .+("SN").greet("ST")
  val p2 = new Person("Pubali", 27)
  println(p2 + p1)
  println((+p1).age)
  println(p2 learnScala)
  p1()
  p2(7)
}


