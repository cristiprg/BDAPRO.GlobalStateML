package de.tu_berlin.dima.bdapro.flink.palindrome.jensmeiners

import org.apache.flink.api.scala._

/**
  * Created by jens on 17.10.16.
  *
  * A file called Palindrome.scala.
  * The file should contain an object called Palindrome with a main method that
  *
  *   looks at its parameter to find the input file,
  *   executes one or more Flink jobs, and
  *   prints the result described in the Output section.
  */
object Palindrome {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val out = env.readTextFile(inputPath)
      // split lines
      .flatMap(_.toLowerCase.split("\\n"))
      // check for palindrome, save its length or set length to zero if its not a palindrome
      .map(x => (x, x.replace(" ", "")))
      .map(x => if (x._2.equals(x._2.reverse)) (x._1, x._2.length) else (x._1, 0))
      // reduce to max
      .reduce((x, y) => if (x._2 == y._2) (x._1+"\n"+y._1, x._2) else if (x._2 > y._2) x else y)
      .flatMap(x => x._1.split("\n"))
      .collect().foreach(x => println("The biggest palindrome sentence: <"+x+">"))
  }
}