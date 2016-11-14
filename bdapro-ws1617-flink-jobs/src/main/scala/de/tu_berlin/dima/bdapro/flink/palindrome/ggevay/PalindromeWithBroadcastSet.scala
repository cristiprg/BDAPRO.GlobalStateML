package de.tu_berlin.dima.bdapro.flink.palindrome.ggevay

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object PalindromeWithBroadcastSet {

  def main(args: Array[String]): Unit = {
    val inFile = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val sent = env.readTextFile(inFile).
      map(o => (o, o.replaceAll(" ", ""))).
      filter(t => t._2 == t._2.reverse).
      map(t => (t._1, t._2.length))

    val maxLen = sent.max(1)

    // see https://cwiki.apache.org/confluence/display/FLINK/Variables+Closures+vs.+Broadcast+Variables
    val allMax = sent.
      filter(new RichFilterFunction[(String, Int)] {
        var maxLenLocal: Int = _
        override def open(parameters: Configuration): Unit = {
          maxLenLocal = getRuntimeContext.getBroadcastVariable[(String, Int)]("maxLength").get(0)._2
        }
        override def filter(t: (String, Int)): Boolean = t._2 == maxLenLocal
      }).withBroadcastSet(maxLen, "maxLength")

    allMax.map(s => s"The biggest palindrome sentence: <${s._1}>").print()
  }
}
