package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.plehmann93

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.util.control.Breaks

/**
  * Created by PA-Lehmann on 24.10.2016.
  */
object OddSemordnilaps {


  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val result=env.readTextFile(inputPath)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(x => x.toInt%2==1)
      .map{x=>if (x<x.reverse) (x,x) else (x.reverse,x) }
      .groupBy(0)
      .reduceGroup { (in, out: Collector[(String)]) =>
        var value:String="null"
        Breaks.breakable{ for (x <- in){
          if(value=="null") {
            value=x._2
            if(value==value.reverse) {
              out.collect(value)
              Breaks.break
            }
          }
          else{
            if(value.reverse==x._2){
              out.collect(value)
              out.collect(x._2)
              Breaks.break()
            }
          }
        }
        }}
    Console.println("The result is "+result.collect().size.toString)
    // env.execute("bdapro-ws1617-flink")
  }

}
