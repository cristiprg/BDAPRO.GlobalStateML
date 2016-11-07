package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.thaohtp

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
  * Created by JML on 11/7/16.
  * Warm up task: Odd Semordnilap Number
  */
object OddSemordnilaps {
  def main(args: Array[String]): Unit = {
    if (args.length <1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }
    val inputFilePath = args(0);

    // set up environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet1 = env.readTextFile(inputFilePath)

    // transform data
    val transformData = dataSet1.flatMap(_.split("\n")).flatMap(_.split(" "))
      .distinct()
      .filter(x => filterOddNumber(x))
      // create reversed number and add to list
      .flatMap(x => Array(x, x.reverse))
      .map(x => (x, 1))

    val groupedDataSet = transformData
      // group by number string
      .groupBy(0)
      // if group has more than 2 members => means that they have reversed number in data set
      // => keep that group and count as 1
      .reduceGroup {
      (in, out: Collector[(Int)]) => {
        if (in.size >= 2) {
          out.collect(1)
        }
      }
    }

    val result = groupedDataSet.count()
    println("The result is " + result)

  }

  def filterOddNumber(numberStr: String): Boolean = {
    if (numberStr == null || numberStr.isEmpty) {
      false
    }
    else {
      if (numberStr.charAt(0).toInt % 2 == 0) {
        false
      }
      else {
        if (numberStr.charAt(numberStr.length -1).toInt % 2 == 0) {
          false
        }
        else {
          true
        }
      }
    }
  }
}
