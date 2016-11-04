package de.tu_berlin.dima.bdapro.flink.palindrome.mschwarzer

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
  * Solution for https://github.com/TU-Berlin-DIMA/BDAPRO.WS1617/issues/2
  *
  * @author mschwarzer
  */
object Palindrome {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0) // Input will contain only lower-case English letters, spaces, newlines and numeric chars

    val env = ExecutionEnvironment.getExecutionEnvironment

    // Read sentences from text file
    val pals = env.readTextFile(inputPath)
      .flatMap{
        (in, out: Collector[(String, Int)]) =>
          val p = in.replaceAll("[^A-Za-z0-9]", "")
          if (p.equals(p.reverse)) { // check for valid palindrome
            out.collect(in, p.length())
          }
      }

    // Save max length in config
    val config = new Configuration()
    config.setInteger("maxLength", pals.max(1).collect().last._2)

    // Filter by max length
    val maxPals = pals
      .filter(new RichFilterFunction[(String, Int)]() {
        var maxLength = 0

        override def open(config: Configuration): Unit = {
          maxLength = config.getInteger("maxLength", 0)
        }

        def filter(in: (String, Int)): Boolean = {
          in._2 == maxLength
        }
      })
      .withParameters(config)
      .collect()

    // Print all left-over sentences
    maxPals.foreach { e =>
      val (sentence, len) = e
      println("The biggest palindrome sentence: <" + sentence + ">")
    }

  }
}
