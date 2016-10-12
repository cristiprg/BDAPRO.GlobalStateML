package de.tu_berlin.dima.bdapro.flink.palindrome.ggevay

import org.apache.flink.api.scala._

object Palindrome {

  def main(args: Array[String]): Unit = {
    val inFile = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val sent = env.readTextFile(inFile).
      map(o => (o, o.replaceAll(" ", ""))).
      filter(t => t._2 == t._2.reverse).
      map(t => (t._1, t._2.length))
    val maxLen = sent.max(1).collect().head._2
    val allMax = sent.filter(t => t._2 == maxLen).
      map(s => s"The biggest palindrome sentence: <${s._1}>")
    allMax.print()
  }
}
