package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.royd1990
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
/**
  * Created by royd1990 on 10/24/16.
  */
object OddSemordnilaps {
  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath outputPath")
      System.exit(-1)
    }
    val inputPath = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val count1 = env.readTextFile(inputPath).flatMap{_.split("\\W+")}.map { i => i.toInt }.distinct().filter(_%2==1)
    val count2 = count1.map{x=>x.toString.reverse }.map{x=> x.toInt}
    val count3 = count2.union(count1).map { (_, 1) }.groupBy(0).sum(1).filter((x) => x._2 == 2)
    print(count3.count)
}
}
