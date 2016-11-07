package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.omarflores16

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

object OddSemordnilaps {

  def main(args: Array[String]){

    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath outputPath")
      System.exit(-1)
    }

    val inputPath = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment

    val outText = env.readTextFile(inputPath)
      .flatMap( _.toLowerCase.trim.split("\\W+") filter { _.nonEmpty } )
      .map( strNumber => (strNumber.reverse,1,(strNumber.reverse.toInt%2)))
      .groupBy(0)
      .sum(1)
      .reduceGroup{
        (in, out: Collector[(String)]) =>
          var i = 0
          for (t <- in) {
            if (t._3 == 1) i = i +1
          }
          out.collect("The result is " + i)
      }
    outText.print()
  }
}
