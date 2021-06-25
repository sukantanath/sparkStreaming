package org.sn.oop

object Inheritence_Train_SN {
  def main(args: Array[String]) = {
    val list1 = new EmptyList()
    val list2 = new NonEmptyList(1,new NonEmptyList(2,new NonEmptyList(3,list1)))

    println(list2.add(4).tail.head)
    //println(list1.head)
    println(list2.printElements)

    val list3 = new NonEmptyList("Hi",new NonEmptyList("How",new NonEmptyList("are", new NonEmptyList("you", list1))))
    println(list3.printElements)
  }
}


abstract class MyList[+A] {

  def head : A
  def tail : MyList[A]
  def isEmpty : Boolean
  def add[B >: A](element: B) : MyList[B]
  def printElements: String
  override def toString: String = "{" + printElements + "}"
}

class EmptyList extends MyList[Nothing]{
  def head : Nothing = throw new NoSuchElementException
  def tail : MyList[Nothing] = new EmptyList().asInstanceOf[MyList[Nothing]]
  def isEmpty : Boolean = true
  def add[B >: Nothing](element:B) : MyList[B] = new NonEmptyList(element,new EmptyList())
  def printElements: String = ""
}

class NonEmptyList[+A](h:A,t:MyList[A]) extends MyList[A] {
  def head : A = h
  def tail : MyList[A] = t
  def isEmpty : Boolean = false
  def add[B >: A] (element:B) : MyList[B] = new NonEmptyList(element,this)
  def printElements: String =
    if(t.isEmpty) ""+h
    else h + " " + t.printElements
}