package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.serkanali

import org.apache.flink.api.scala._

/**
  * Created by nerka on 31.10.2016.
  */
object OddSemordnilaps {
  def main(args: Array[String]): Unit = {
    if(args.length < 1)
      System.exit(-1)

    val inputPath = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment

  val allNumbersAsString= env.readTextFile(inputPath)
    .flatMap(_.toLowerCase.split("\\W+"))

  val allNumbers = allNumbersAsString
    .map(value => value.toLong)

  val allReserveOddNumbers = allNumbers
    .map(value => ReseversNumber(value))
    .filter(value => value % 2 == 1)
  val Result = allNumbers
    .join(allNumbers)
    .where(value => value)
    .equalTo(value => ReseversNumber(value))
    .filter(value => value._1 % 2 == 1 && value._2 % 2 == 1)
    .map((_,1,1))
    .groupBy(0)
    .sum(1)
    .sum(2)
    .map(value => value._3)
    .print()
  }

  def ReseversNumber(digit : Long): Long ={
    var number = digit
    var ReverseNmb: Long = 0
    while (number > 0){
      ReverseNmb *= 10
      ReverseNmb += number % 10
      number /= 10
    }
    return ReverseNmb
  }
}

