package de.tu_berlin.dima.bdapro.flink.palindrome.wladox

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

object Palindrome {

  def main(args: Array[String]) {

    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val REGEX = "[_[^\\w\\d]]"

    val inputPath = args(0)

    val myFilter = (s: String) => isPalindrome(s)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)

    val result = text
      //.flatMap(m => m.split("[\\r?\\n]"))
      .filter(myFilter)
      .map(s => MyTuple(s, s.replaceAll(" ", "").length))
      .reduceGroup {
        (in, out: Collector[MyTuple]) =>
          val list = in.toList
          val max = list.maxBy(_.length)
          list foreach (e => if (e.length == max.length) out.collect(e))
      }

    result.print()
  }

  def isPalindrome(s:String):Boolean = {
    val REGEX = "[_[^\\w\\d]]"
    s.nonEmpty && s.replaceAll(REGEX, "") == s.replaceAll(REGEX, "").reverse
  }

  case class MyTuple(text:String, length:Int) {
    override def toString:String= s"The biggest palindrome sentence: <$text>"
  }


}

