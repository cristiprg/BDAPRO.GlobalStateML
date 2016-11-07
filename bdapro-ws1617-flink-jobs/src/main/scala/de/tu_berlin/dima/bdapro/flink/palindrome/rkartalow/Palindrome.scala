
package de.tu_berlin.dima.bdapro.flink.palindrome.rkartalow
import org.apache.flink.api.scala._

/**
  * Created by robert.kartalow on 05.11.2016.
  */
object Palindrome {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[(String,Int)] = env.readTextFile(inputPath)
      .flatMap(_.split("\n"))
      .filter(isPalindrome(_))
      .map{x => (x, x.length)}

    val max: DataSet[Int] = data.max(1)
      .map(x=>x._2)

    data.cross(max)
      .filter(isMax(_))
      .map(x => "The biggest palindrome sentence: <"+ x._1._1 +">")
      .print()

    //env.execute("bdapro-ws1617-flink")
  }

  def isPalindrome(s:String):Boolean = {
    val f = s.replaceAll(" ","")
    f == f.reverse
  }

  def isMax(a: ((String,Int), Int)):Boolean = {
    a._1._2 == a._2
  }

}