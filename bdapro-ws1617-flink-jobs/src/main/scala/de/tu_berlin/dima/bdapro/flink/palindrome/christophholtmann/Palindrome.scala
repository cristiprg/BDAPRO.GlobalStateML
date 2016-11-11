package de.tu_berlin.dima.bdapro.flink.palindrome.christophholtmann

import org.apache.flink.api.scala._

/**
  * Created by christopholtmann on 24-Oct-16.
  */
object Palindrome
{
  def main(args : Array[String])
  {
    val env = ExecutionEnvironment.getExecutionEnvironment;

    val input = env.readTextFile(args{0});

    val possiblePalindromes = input.flatMap(_.split("\\n"))
      .map(i => (i, getWithoutWhitespace(i)))
      .filter(i => isPalindrome(i._2))
      .map(p => (p._1, p._2.length))

    val longestPalindromeLength = possiblePalindromes
      .reduce((p, q) => if (p._2 > q._2) p else q)
      .map(p => p._2)
      .collect()

    possiblePalindromes
      .filter(p => p._2 == longestPalindromeLength(0))
      .map(a => "The biggest palindrome sentence: < " + a._1 + " >")
      .print()
  }

  def getWithoutWhitespace(input : String) : String =
  {
    input.replace(" ", "")
  }

  def isPalindrome(input : String) : Boolean =
  {
    input == input.reverse
  }
}