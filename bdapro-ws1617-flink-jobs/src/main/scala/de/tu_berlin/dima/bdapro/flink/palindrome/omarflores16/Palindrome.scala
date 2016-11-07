package de.tu_berlin.dima.bdapro.flink.palindrome.omarflores16

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.Queue

object Palindrome {
  def main(args: Array[String]){

    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath outputPath")
      System.exit(-1)
    }

    var maxStringPalindrome = 0
    val inputPath = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment

    val lineText = env.readTextFile(inputPath)
      .flatMap( _.toLowerCase.trim.split("\\n") filter { _.nonEmpty } )
      .map{ strPalindrome =>  (strPalindrome.size,strPalindrome,palindromeFunc(strPalindrome))}

    for (t <- lineText.max(0).collect())
      maxStringPalindrome = t._1

    val finalTuple = lineText
      .reduceGroup{
        (in, out: Collector[(String)]) =>
          for (t <- in) {
            if (t._3 && t._1 >= maxStringPalindrome)
              out.collect("The biggest palindrome sentence: " + t._2)
          }
      }
    finalTuple.writeAsText("Output")
    env.execute("bdapro-ws1617-flink")
  }

  def palindromeFunc(phrase: String): Boolean ={
    var qPalindrome = new Queue[(Char)]
    var isEqually: Boolean = true

    (phrase).foreach( (w: Char) => if (!w.isSpaceChar)
      qPalindrome.enqueue(w.toLower) )

    while(qPalindrome.size > 1 && isEqually){
      val wFirst = qPalindrome.dequeue().toString
      qPalindrome = qPalindrome.reverse
      val wLast =  qPalindrome.dequeue().toString
      qPalindrome = qPalindrome.reverse

      if (wFirst != wLast)
        isEqually = false
    }

    return isEqually
  }

}
