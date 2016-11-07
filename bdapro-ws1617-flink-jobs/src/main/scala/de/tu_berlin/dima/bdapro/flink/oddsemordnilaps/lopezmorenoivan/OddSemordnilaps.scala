package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.lopezmorenoivan

import org.apache.flink.api.scala._

object OddSemordnilaps {

  def isOdd(number: Int) = number % 2 != 0

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment

    val keys = env.readTextFile(inputPath)
      .flatMap(_.split("\\W+"))
      .distinct
      .collect

    val result = keys
      .filter(x=> isOdd(x.toInt) && isOdd(x.reverse.toInt) && keys.contains(x.reverse))
      .size

    print("The result is "+result)
  }

}
