package de.tu_berlin.dima.bdapro.flink
import org.apache.flink.api.scala._

object OddSemordnilaps {

  def main(args: Array[String]) {
    // What to do with input path HDFS (when it starts with hdfs://)?
    val inputPath = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(inputPath)
      .flatMap {_.split(" ")}
      .map(x => x.toInt)
      .filter(x => (x%2 == 1))
      .map(x => (x.toString, 1))

    val text1 = text.map(x => (x._1.reverse, 1))

    val text2 = text1.join(text).where(0).equalTo(0)
      .map(x => (x._1))
      .groupBy(0)
      .sum(1)
      .map(x =>(x._1,1))
      .sum(1)
      .map(x => "The result is " + x._2.toString)

    text2.print()
  }
}

