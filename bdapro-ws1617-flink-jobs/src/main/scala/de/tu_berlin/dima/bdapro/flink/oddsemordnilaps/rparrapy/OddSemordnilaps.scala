package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.rparrapy

import org.apache.flink.api.java.aggregation.Aggregations._
import org.apache.flink.api.scala._

/**
  * Created by rparra on 21/10/16.
  */
object OddSemordnilaps {


  def main(args: Array[String]): Unit = {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val lines = env.readTextFile(args(0))

    val result = lines.flatMap{ _.trim.split("\\W+") }
      .distinct
      .map{ Integer.parseInt(_) }
      .filter{ _ % 2 != 0}
      .flatMap{ (x: Int) => x :: Integer.parseInt(Integer.toString(x).reverse) :: Nil }
      .filter{ _ % 2 != 0}
      .map{ (_, 1) }
      .groupBy(0)
      .aggregate(SUM, 1)
      .filter{ _._2 == 2 }
      //.map{ _._1 }
      .sum(1)
      .map( "The result is " + _._2 / 2  + "\n")



    result.print()
  }

}