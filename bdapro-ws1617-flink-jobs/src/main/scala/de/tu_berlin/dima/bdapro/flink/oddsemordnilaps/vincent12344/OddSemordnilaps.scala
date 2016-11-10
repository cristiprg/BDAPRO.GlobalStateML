package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.vincent12344

import org.apache.flink.api.scala._

object OddSemordnilaps {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val input = env.readTextFile(args{0})

    val distinctOddNumbers = input.flatMap { _.split("\\n") }
      .flatMap {_.split(" ")}
      .filter {a => a.length != 0 && a.toInt % 2 == 1}
      .distinct

    val semoCount = distinctOddNumbers.cross(distinctOddNumbers)
      .filter {a => a._1.equals(a._2.reverse)}
      .count

    print("The result is " + semoCount)
  }
}
