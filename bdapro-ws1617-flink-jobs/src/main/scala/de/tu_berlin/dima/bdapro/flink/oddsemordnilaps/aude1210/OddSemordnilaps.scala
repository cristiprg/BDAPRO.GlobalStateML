package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.aude1210
import org.apache.flink.api.scala._
/**
  * Created by Aude on 01/11/2016.
  */
object OddSemordnilaps {
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.exit(-1)
    }
    val inputPath = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val semordnilaps = env.readTextFile(inputPath).flatMap(_.split("\\D+")).distinct()
      .filter(x => x!="" && x.toInt%2==1 && x.reverse.toInt%2==1)
    val nb = semordnilaps.cross(semordnilaps).filter(x=> x._1.reverse==x._2).count()
    println("The result is "+nb)
  }
}
