package de.tu_berlin.dima.bdapro.flink.palindrome.vincent12344

import org.apache.flink.api.scala._

object Palindrome {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val input = env.readTextFile(args{0})
      .flatMap {_.split("\\n")}
      .filter {str => str.replaceAll(" ", "").equals(str.replaceAll(" ", "").reverse)}
      .map {str => (str, str.replaceAll(" ", "").length)}

    val maxPalindromLength = input
      .max(1)
      .collect

    val longestPalindromes = input
      .filter {a => a._2 == maxPalindromLength{0}._2}
      .map {str => "The biggest palindrome sentence: <" + str._1 + ">"}
      .print
  }
}
