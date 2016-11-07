package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.kskaranra
import org.apache.flink.api.scala._

/**
  * Created by karan on 11/6/16.
  */
object OddSemordnilaps {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath outputPath")
      System.exit(-1)
    }

    val inputPath =args(0)
    //val outputPath = args(1)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data1 = env.readTextFile(inputPath)
      .flatMap(_.toLowerCase.split("\\W+"))
      .distinct
      .filter(check(_) == 1)
      .map(x => (x, add(x)))
      .map(x => (x._1, x._2, checkP(x._1)))
      .groupBy(1)
      .sum(2)
      .filter(_._3 == 2)
      .map(x => (x._1, final1(x._1)))
      .sum(1).map(x => (x._2)).print()



  }

  def check(a: String): Int = {
    val number = a
    if (isOdd(number.toInt)) {
      return 1
    }
    else {
      return 0
    }
  }

  def checkP(str: String): Int = {
    val num = str
    val num2 = str.reverse
    if (num == num2) return 2
    else return 1
  }

  def final1(a: String): Int = {
    val number = a
    val number1 = a.reverse
    if (number == number1) return 1
    else return 2
  }

  def isOdd(number: Int) = number % 2 != 0

  def add(a: String): Int = {
    val number = a
    val number1 = a.reverse
    return (number.toInt + number1.toInt)
  }
}