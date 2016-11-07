package de.tu_berlin.dima.bdapro.flink.palindrome.kskaranra
import org.apache.flink.api.scala._
object Palindrome {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath outputPath")
      System.exit(-1)
    }
    val inputPath = args(0)
    // val outputPath = args(1)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val temp = env.readTextFile(inputPath).flatMap(_.toLowerCase.split("\\n+")).distinct.
      map((s:String) => (s, calc(s), isPalindrome(s)))
      .filter(_._3 == 1)
      .collect()
    val temp2 = temp.map(x => x._2)

    val maxL = temp2.maxBy(_.__leftOfArrow)
    temp.filter(x => x._2 == maxL).
      foreach { x=> val y= x._1
        println(s"The biggest palindrome sentence is: <$y>")
      }

  }

  def isPalindrome(l: String): Int = {
    val text = l.replaceAll("[^A-Za-z0-9]", "")
    if (text == text.reverse) {
      return 1
    }
    else {
      return 0
    }
  }
  def calc(a: String): Int = {
    val text = a.replaceAll("[^A-Za-z0-9]", "")
    return text.length
  }
}