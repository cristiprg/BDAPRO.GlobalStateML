package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.heytitle

import org.apache.flink.api.scala._

/**
  * Created by heytitle on 10/22/16.
  */
object OddSemordnilaps {
  val file = "file:///Users/heytitle/projects/BDAPRO.WS1617/bdapro-ws1617-bundle/src/main/resources/datasets/evenSmallerOddsemordinalaps.txt"

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
    }

    val inputPath = if( args.length == 1 ) args(0) else file

    val env = ExecutionEnvironment.getExecutionEnvironment

    val numbers = env.readTextFile(inputPath)
      .flatMap( _.split(" ") )
      .distinct()
        .filter( _.toLong % 2 != 0 )
          .map( ( _,1) );

    val oddSem = numbers
      .map( n => (n._1, n._1.reverse))
      .join(numbers).where(1).equalTo(0)

    val total =  oddSem.count()
    println(s"The result is $total")
  }

}
