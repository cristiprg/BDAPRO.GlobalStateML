package de.tu_berlin.dima.bdapro.flink.palindrome.uybhatti

import org.apache.flink.api.scala._

object Palindrome {


  def getPalindromeLength(sentence: String): Integer = {

    //val cleanSentence = sentence.replaceAll("[^a-zA-Z0-9]","")
    val cleanSentence = sentence.replaceAll("\\P{Alnum}", "")

    if( cleanSentence.reverse == cleanSentence)
      return  cleanSentence.length
    else
      return 0;


  }

  def main(args: Array[String]) {

    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment

    val sentencesWithPalindromeLength = env.readTextFile(inputPath).
      flatMap(_.toLowerCase.split("\\n")).
      map ( (x:String) => (x,getPalindromeLength(x))).
      distinct.
      collect()

    val maxLength = sentencesWithPalindromeLength.maxBy( x=> x._2)._2

    sentencesWithPalindromeLength.filter( x => x._2 == maxLength).
      foreach{
        x => val biggestPalindromSentence = x._1
        println(s"The biggest palindrome sentence: <$biggestPalindromSentence>") }

  }


}