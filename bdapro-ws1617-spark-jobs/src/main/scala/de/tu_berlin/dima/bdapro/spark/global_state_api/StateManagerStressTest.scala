package de.tu_berlin.dima.bdapro.spark.global_state_api

import java.io._
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by cristiprg on 13.03.17.
  */
object StateManagerStressTest{

  private var stateMatrix: Array[Vector] = null

  private case class TestResultType(N: Int, mean: Double, stddev: Double, data: Array[Long])

  private def printResult(result: TestResultType): Unit = {
    println(f"N = ${result.N}")
    println(f"Mean = ${result.mean}%.3f")
    println(f"Stddev = ${result.stddev}%.3f")
  }

  private def printResultSuccint(nrRows: Int, nrCols: Int, result: TestResultType): Unit = {
    println(f"${nrRows}%d,${nrCols}%d,${result.mean}%.3f,${result.stddev}%.3f")
  }

  private def exportResultCSV(result: TestResultType, filename: String): Unit = {
    val pw = new PrintWriter(new File(filename))

    // Write header
    pw.write("N, duration, mean, stddev\n")

    // Write values
    for (i <- result.data.indices)
      pw.write(s"$i, ${result.data(i)}, ${result.mean}, ${result.stddev}\n")

    pw.close()
  }

  private def generateState(nrRows: Int, nrCols: Int) : Array[Vector] = {
    val array = new Array[Vector](nrRows)
    val r = scala.util.Random

    for (i <- 0 until nrRows) {
      val vectorArray = new Array[Double](nrCols)

      for (j <- vectorArray.indices) {
        vectorArray(j) = r.nextDouble() + 0.1
      }

      array(i) = Vectors.dense(vectorArray)
    }

    array
  }

  /**
    * http://stackoverflow.com/questions/9160001/how-to-profile-methods-in-scala
    * @param block statement for which to measure time
    * @tparam R return type of the block
    * @return a tuple containing the result of the bock and its execution time in nanoseconds
    */
  def time[R](block: => R): (Long) = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()

    (t1-t0)
  }

  /**
    *
    * @param nrRepetitions
    * @param block
    * @tparam R
    * @return a tuple containing information about the runs: (nrRepetitions, Mean, Variance)
    */
  private def timeWithStats[R](nrRepetitions: Int)(block: => R): TestResultType = {
    val durations = new Array[Long](nrRepetitions)

    for (i <- 0 until nrRepetitions) {
      durations(i) = time { block }
    }

    val mean = durations.sum.toDouble / nrRepetitions
    val devs = durations.map(duration => (duration - mean) * (duration - mean))
    val variance = devs.sum / (nrRepetitions - 1)

    TestResultType(nrRepetitions, mean, Math.sqrt(variance), durations)
  }


  def main(args: Array[String]): Unit = {

    val usage = "Usage: StateManagerStressTest <redis server> <nrRepetitions> <nrRows> <nrCols> <outputCsvFile> <command>\n"

    try { args(0); args(1).toInt; args(2).toInt; args(2).toInt; args(3); args(4); args(5)}
    catch { case _: Exception => println(usage); System.exit(1); }

    val redisServerAddress: String = args(0)
    val nrRepetitions: Int = args(1).toInt
    val nrRows: Int = args(2).toInt
    val nrCols: Int = args(3).toInt
    val outputCsvFile: String = args(4)
    val operation: String = args(5)


    val stateManager: StateManager = new StateManager(redisServerAddress)
    stateMatrix = generateState(nrRows, nrCols)

    var result: TestResultType = null

    operation match {
      case "SAVE" =>
        result = timeWithStats(nrRepetitions) { stateManager.setState(stateMatrix) }
      case "LOAD" =>
        stateManager.setState(stateMatrix)
        result = timeWithStats(nrRepetitions) { stateManager.getStateArrayOfVectors() }
      case whoa =>
        println("Invalid operation: " + whoa)
        System.exit(2)
    }

    //printResult(result)
    printResultSuccint(nrRows, nrCols, result)
    exportResultCSV(result, outputCsvFile)
  }
}
