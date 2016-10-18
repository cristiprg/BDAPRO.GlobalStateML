package de.tu_berlin.dima.bdapro.flink.palindrome.heytitle

import org.apache.flink.api.scala._

/**
  * Created by heytitle on 10/19/16.
  */
object Palindrome {
  val file = "file:///Users/heytitle/projects/BDAPRO.WS1617/bdapro-ws1617-bundle/src/main/resources/datasets/palindromeSmall.txt"
  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
    }

    val inputPath = if( args.length == 1 ) args(0) else file
    val env = ExecutionEnvironment.getExecutionEnvironment
    val palSentences = env.readTextFile(inputPath)
      .map( s => Sentence(s) )
      .filter( _.isPalindrome )

    palSentences.print()

    val biggestSentence = palSentences.reduce{
      (s1,s2) => if(s1.length > s2.length ) s1 else s2
    }.collect()(0).length;

    val results = palSentences.filter( _.length == biggestSentence )

    results.print()

  }

  case class Sentence(s: String){
    private val ns = s.replaceAll(" ", "")
    val isPalindrome = ns.reverse == ns
    val length = ns.length

    override def toString: String = {
      return s"The biggest palindrome sentence: <$s>"
    }
  }
}
