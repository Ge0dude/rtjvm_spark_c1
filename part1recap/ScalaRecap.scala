package part1recap

class ScalaRecap extends App {
  //val non-mutable, var mutable
  val aBoolean: Boolean = false

  val anIfExpression = if(2>3) "bigger" else "smaller"

  //instructions vs expressions
  val theUnit = println("Hello, Scala") // has type Unit--no meaningful value (void in other languages)

  //functions
  def myFunction(x: Int) = 42

  //OOP
  class Animal
  class Dog extends Animal
  trait Carnivorne {
    def eat(animal: Animal): Unit
  }
  class Crocodile extends Animal with Carnivorne {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }
  //singleton objects
  object mySingleton
  //companion
  object Carnivorne
  //generic
  trait MyList[A]
  //method notation
  // plus is technically a method, so it can be called two ways
  val x = 1 + 2
  val y = 1.+(2)

  //functional programming
}
