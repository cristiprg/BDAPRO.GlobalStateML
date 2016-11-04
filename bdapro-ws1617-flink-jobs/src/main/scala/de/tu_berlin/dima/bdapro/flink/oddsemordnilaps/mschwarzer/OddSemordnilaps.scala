package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.mschwarzer

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

/**
  * Warm Up Tasks https://github.com/TU-Berlin-DIMA/BDAPRO.WS1617/issues/3
  *
  * @author mschwarzer
  */
object OddSemordnilaps {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0) // Input will contain only numerical characters, spaces, and newlines

    val env = ExecutionEnvironment.getExecutionEnvironment

    val odds = env.readTextFile(inputPath)
      .flatMap{
        (in, out: Collector[(Int, Int)]) =>
          in.split("\\W+").foreach { str =>
            val i = str.toInt

            if ( i % 2 != 0) { // Only odd numbers
              val first = str.charAt(0).toInt
              val last = str.charAt(str.length()-1).toInt
              var reverse = str.reverse.toInt


              if (i == reverse) { // Is self-semordnilaps (7, 55, 999, ...)
                out.collect(i, -1)
              } else if (last > first) { // Reverse if in desc order
                out.collect(reverse, 1)
              } else {
                out.collect(i, 0)
              }
            }

          }
      }
    val sems = odds
      .groupBy(0)
      .reduceGroup{
        (in, out: Collector[(Int, Int)]) =>
          var selfsems, desc, asc, groupi = 0 // Init counters

          in.foreach { element => // Loop over group elements
            val (i, order) = element
            groupi = i

            if(order == -1) { // Always count self-semordnilaps
              selfsems += 1
            } else if(order == 1) {
              desc += 1
            } else if(order == 0) {
              asc += 1
            }

          }
          // If not self-semordnilaps, one of each order needs to exist
          val total = selfsems + (if (desc > 0 && asc > 0) 1 else 0)

          if (total > 0) {
            out.collect(groupi, total)
          }
      }
      .sum(1)
      .collect()

    println("The result is " + sems.last._2)

  }
}
