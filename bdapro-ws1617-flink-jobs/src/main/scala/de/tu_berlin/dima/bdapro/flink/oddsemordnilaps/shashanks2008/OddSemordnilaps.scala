package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.shashanks2008

import org.apache.flink.api.scala._
object OddSemordnilaps {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath outputPath")
      System.exit(-1)
    }
    val inputPath = args(0)
    //val outputPath = args(1)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data1 = env.readTextFile(inputPath)
      .flatMap(_.toLowerCase.split("\\W+"))
      .distinct
      .filter(check(_)== 1).filter(check2(_)== 1) //filter all the numbers which are odd and their reverse is also odd
      .map(x => (x,add(x)))         //added all numbers and their reverse so the number which has semordnilaps will have 2
      .map(x => (x._1,x._2,checkP(x._1))).groupBy(1).sum(2)
      .filter(_._3==2)
      .map(x => (x._1, final1(x._1)))
      .sum(1).map(x=>(x._2))
    val data2 = data1
    data2.map("The result is: "+_).print()
  }
  def check(a: String): Int = {val number = a
    if (isOdd(number.toInt)) {return 1}
    else {return 0}}

  def check2(a: String): Int = {val number = a.reverse
    if (isOdd(number.toInt)) {return 1}
    else {return 0}}

  def checkP(a: String): Int = {
    val number = a
    if(number == number.reverse) return 2
    else return 1
  }
  def final1(a: String): Int = {
    val number = a
    val number1 = a.reverse
    if(number == number1) return 1
    else return 2
  }
  def isOdd(number: Int) = number % 2 != 0

  def add(a: String): Int = {
    val number = a
    val number1 = a.reverse
    return (number.toInt + number1.toInt)
  }
}