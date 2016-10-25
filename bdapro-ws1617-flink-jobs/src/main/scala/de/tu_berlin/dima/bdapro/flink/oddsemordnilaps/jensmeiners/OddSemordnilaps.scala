package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.jensmeiners

import org.apache.flink.api.scala._

/**
  * Created by jens on 25.10.16.
  */
object OddSemordnilaps {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val origin = env.readTextFile(inputPath)
      // split lines and spaces
      .flatMap(_.toLowerCase.split("[\\n ]"))
      .distinct
      .filter(isOddSem(_))
    val out = origin.cross(origin.map(_.reverse))
      .apply((x, y) => if (x.equals(y)) 1 else 0)
      .reduce((x, y) => x + y)
      .collect.foreach(x => println("The result is " + x))

  }

  def isOddSem(s:String): Boolean = {
    return s.charAt(0).toInt % 2 != 0 && s.charAt(s.length-1).toInt % 2 != 0
  }
}
