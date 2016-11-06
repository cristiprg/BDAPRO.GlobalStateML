package de.tu_berlin.dima.bdapro.flink.palindrome.plehmann93


import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.math.Ordering.String
import scala.util.control.Breaks
/**
  * Created by PA-Lehmann on 22.10.2016.
  */
object Palindrome {



  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }
    val inputPath = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val result= env.readTextFile(inputPath)
      .flatMap(_.toLowerCase.split("/n"))
      .filter(x => x.replace(" ","")==x.replace(" ","").reverse)
      .map( x => (1,x.replace(" ","").size,x)  )
      .groupBy(0).sortGroup(1,Order.DESCENDING)
      .reduceGroup{ (in, out: Collector[(Int, Int, String)]) =>
      var max: Int = -1
      Breaks.breakable{for (x <- in){
        if(max==(-1)) {max=x._2}
        if(max>x._2){Breaks.break()}
        out.collect(x)
      }}
    }
    result.collect().foreach(x=>Console.println("The biggest palindrome sentence: <"+x._3+">"))
    //env.execute("bdapro-ws1617-flink")
  }
}