package de.tu_berlin.dima.bdapro.flink.palindrome.cristiprg

import org.apache.flink.api.scala._

/** Solution to the palindrome warm-up task. I'm aware this is by far not the best solution. I'm looking forward to
  * learning how to do this with less jobs. */
object Palindrome {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)
    val stringPrefix = "The biggest palindrome sentence: <"
    val stringSuffix = ">"

    val env = ExecutionEnvironment.getExecutionEnvironment
    val textFile = env.readTextFile(inputPath)

    // Flink job 1: get all the palindromes
    val palindromes = textFile
      .map(x => {
        val noSpaceLine = x.replaceAll("[ \\t\\r\\f]", "")
        (x, noSpaceLine == noSpaceLine.reverse, x.length)     // embed information about being palindrome and length
      })
      .filter(x => x._2)                                      // filter out what's not palindrome
      .collect()                                              // execute job

    // Flink job 2: get max length of palindromes
    val max = env.fromCollection(palindromes)                 // cast Seq to DataSet
      .max(2)                                                 // aggregate on max
      .collect()                                              // execute job
      .head._3                                                // get only the Int value

    // Flink job 3: filter out palindromes of len < max
    env.fromCollection(palindromes)
      .filter(x => x._3 == max)                               // filter our what's not max length
      .map(x => stringPrefix + x._1 + stringSuffix)           // prepare final format of the result
      .print()                                                // print to stdout the sentences
  }
}
