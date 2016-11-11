package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.christophholtmann

import org.apache.flink.api.scala._

/**
  * Created by christophholtmann on 03/11/2016.
  */

object OddSemordnilaps {

  def main(args: Array[String]): Unit =
  {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val input = env.readTextFile(args{0})

    val possibleOddSemordnilaps = input.flatMap (_.split("\\n"))
      .flatMap(_.split(" "))
      .filter(i => i.length != 0 && i.toInt % 2 != 0)
      .distinct()
      .map(i => (i, i.reverse))

    val oddSemordnilapsCount = possibleOddSemordnilaps
      .join(possibleOddSemordnilaps)
      .where(0)
      .equalTo(1)
      .count()

    println("The result is " + oddSemordnilapsCount)
  }
}