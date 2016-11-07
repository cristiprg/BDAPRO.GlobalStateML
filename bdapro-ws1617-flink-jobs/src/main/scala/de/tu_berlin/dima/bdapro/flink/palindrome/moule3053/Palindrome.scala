package de.tu_berlin.dima.bdapro.flink.palindrome.moule3053

import org.apache.flink.api.scala._

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Created by mulugeta on 23/10/16.
  */
object Palindrome {

  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data

    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)

    val inputText = env.readTextFile(inputPath)

    //the palindromes
    val palindromes = inputText.flatMap {
      _.split("\\n+")
    }
      .filter(line => line.replace(" ", "") == line.replace(" ", "").reverse)
      .map( line => (line, line.replace(" ", "").length()))

    //calculate the length of the longest palindromes
    val maxSize = palindromes.max(1).map(_._2).collect().head

    //the longest palindromes
    val longPalindromes = palindromes.filter(line => line._2 == maxSize).map(line => line._1)

    //Execute and print result
    longPalindromes.collect.foreach(s => println("The biggest palindrome sentence: <" + s + ">"))


  }

}
