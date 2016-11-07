package de.tu_berlin.dima.bdapro.flink.palindrome.royd1990
import org.apache.flink.api.scala._
object Palindrome {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath outputPath")
      System.exit(-1)
    }

    val inputPath = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = env.readTextFile(inputPath).flatMap(_.toLowerCase.split("\\n")).filter(s=>s.replace(" ","")==s.replace(" ","").reverse) //Palindrome
    val length = input.map(toLength(_)).max(1).map(_._2).collect().head
    val finRes = input.filter(_.length == length).map("Max Palindrome: "+_).print()
  }

  def toLength(l:String) = ((l,l.replace(" ","").length))

}

