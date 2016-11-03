package de.tu_berlin.dima.bdapro.flink

import org.apache.flink.api.scala._

object Palindrome {

  def main(args: Array[String]) {
    val inputPath = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(inputPath)
      .flatMap { _.toLowerCase.split("\n")}
      .map{
        x=> (x, x.replace(" ", ""), x.length)
      }
      .filter{x => x._2 == x._2.reverse}

      val maxval = text.max(2)

      val text1 = text.join(maxval).where(2).equalTo(2)
      .map(x => "The biggest palindrome sentence: <" + x._1._1 + ">" )

    text1.print()

  }
}