package de.tu_berlin.dima.bdapro.flink.palindrome.arne7morgen

import org.apache.flink.api.scala._
import org.apache.flink.api.common.operators.Order

object Palindrome{

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val palindrome = env.readTextFile(inputPath)
      .filter(s => s.split(" ").map(_.trim).mkString("") == 
        s.split(" ").map(_.trim).mkString("").reverse)
      .map(s => (s,s.split(" ").map(_.trim).mkString("").size))
      .reduce((s1, s2) => if (s1._2 == s2._2) (s1._1+","+s2._1, s2._2) 
          else if (s1._2 > s2._2) s1 else s2)
      .flatMap(s => s._1.split(","))
      .collect()
      .foreach(s => println("The biggest palindrome sentence: <"+s+">"))

  }
  
}
