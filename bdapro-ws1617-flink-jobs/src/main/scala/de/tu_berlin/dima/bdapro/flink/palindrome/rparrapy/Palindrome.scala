package de.tu_berlin.dima.bdapro.flink.palindrome.rparrapy

import org.apache.flink.api.scala._

/**
  * Created by rparra on 19/10/16.
  */
object Palindrome {

  def main(args: Array[String]): Unit = {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val lines = env.readTextFile(args(0))

    val result = lines.distinct
      .filter { x => x.nonEmpty && x.replaceAll("[^A-Za-z0-9]", "") == x.replaceAll("[^A-Za-z0-9]", "").reverse }
      .map { x => PalindromeResult(List(x)) }
      .reduce { (x, y) => x.combine(y) }
      .flatMap {
        _.palindromes.map { p => s"The biggest palindrome sentence: <$p>" }
      }

    result.print()
  }

}

case class PalindromeResult(val palindromes: List[String]) {
  def size() = palindromes.head.replaceAll("[^A-Za-z0-9]", "").size

  def combine(other: PalindromeResult): PalindromeResult = {
    if (this.size == other.size) PalindromeResult(this.palindromes ::: other.palindromes)
    else {
      if (this.size > other.size) this
      else other
    }
  }
}