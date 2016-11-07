package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.mmziyad

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * Created by Ziyad on 07/11/16.
  */
object OddSemordnilaps {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath ")
      System.exit(-1)
    }

    val inputPath = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.readTextFile(inputPath)
      .flatMap(_.trim.split("\\s").distinct.filter(_.toInt % 2 != 0))
      .map(d => (d, d.length))
      .groupBy(_._2)
      .reduceGroup {
        (in, out: Collector[(String, Int)]) => {
          val li = in.toSet

          for (e <- li) {
            val x = e
            val y = (e._1.reverse, e._2)

            if (((x._1.reverse == x._1)) || li.contains(y))
              out.collect(x._1, 1)
          }
        }
      }.count()

    println("The result is " + data)
    //env.execute("bdapro-ws1617-flink")
  }

}
