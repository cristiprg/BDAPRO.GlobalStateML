package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.rkartalow

import org.apache.flink.api.scala._

/**
  * Created by robert.kartalow on 07.11.2016.
  */
object OddSemordnilaps {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[String] = env.readTextFile(inputPath)
      .flatMap(_.split("\n"))
      .flatMap(_.split(" "))
      .filter(isOdd(_))

    val result :Long = data.cross(data)
      .filter(isReverse(_))
      .map(x=>x._1)
      .distinct()
      .count()

    println("The result is " + result)
    //env.execute("bdapro-ws1617-flink")
  }

  def isOdd(s: String):Boolean = {
    s.toInt%2==1
  }

  def isReverse(t: (String, String)):Boolean = {
    t._1 == t._2.reverse
  }
}