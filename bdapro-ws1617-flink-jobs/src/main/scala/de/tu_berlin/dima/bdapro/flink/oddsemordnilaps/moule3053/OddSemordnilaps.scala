package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.moule3053

import org.apache.flink.api.scala._

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Created by mulugeta on 23/10/16.
  */
object OddSemordnilaps {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data

    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)

    val inputText = env.readTextFile(inputPath)

    //Leave out only unique odd numbers
    val counts = inputText.flatMap { _.split("\\W+") }
      .map { x => x.toInt }
      .distinct()
      .filter(_ % 2 != 0)

    //New dataset containing the reversed numbers
    val counts2 = counts.map { x => x.toString.reverse}
      .map {x => x.toInt}

    //Combine the two datasets and leave those which appear twice
    val counts3 = counts.union(counts2)
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)
      .filter((x) => x._2 == 2)

    // execute and print result
    print("The result is " + counts3.count())
  }
}
