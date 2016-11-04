package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.cristiprg

import org.apache.flink.api.scala._

/**
  * Created by cristiprg on 3-11-16.
  */
object OddSemordnilaps {

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    val inputPath = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment

    val firstSet = env.readTextFile(inputPath)
      .flatMap(_.toLowerCase.split("\\W+"))     // split by words
      .distinct()                               // remove all the duplicates
      .filter(x => x.toInt % 2 == 1)            // filter out all the even numbers
      .map( x => (x, 1))                        // force having a tuple, so that we can use some function below.
                                                // TODO: find out how to do this without this hack

  val length = firstSet                         // use a second dataset
      .map( x=> (x._1.reverse, 1))              // get the reverse of strings
      .join(firstSet)                           // join on the first element
      .where(0)
      .equalTo(0)
      .count()                                  // the number of lines is the what we're looking for

    print(length)
  }
}
