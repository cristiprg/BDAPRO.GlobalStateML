package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.arne7morgen

import org.apache.flink.api.scala._

object OddSemordnilaps {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val semords = env.readTextFile(inputPath)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(a => a.toInt % 2 == 1 && a.reverse.toInt % 2 == 1) // check for oddity(?)
      .map(a => (a, if (a.toInt < a.reverse.toInt) a.toInt else a.reverse.toInt))

    val semordsCopy = semords.first(semords.count().toInt)
    val semordsJoin = semords.join(semordsCopy).where(1).equalTo(1) { (a, b) => (a._2, 1) }
      .groupBy(0)
      .sum(1)
      .filter(a => a._2 > 1)

    println("The result is " + semordsJoin.count())
  }

}
