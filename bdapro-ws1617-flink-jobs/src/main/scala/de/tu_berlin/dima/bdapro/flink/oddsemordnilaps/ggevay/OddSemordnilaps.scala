package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.ggevay

import org.apache.flink.api.scala._

object OddSemordnilaps {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val numbers = env.readTextFile(inputPath).flatMap(_.split("\\W+")).distinct
    val odds = numbers.filter(n => n.toInt % 2 == 1)
    val semordnilaps = odds.join(odds).where(_.reverse).equalTo(x=>x)
    println(s"The result is ${semordnilaps.count()}")
  }
}
