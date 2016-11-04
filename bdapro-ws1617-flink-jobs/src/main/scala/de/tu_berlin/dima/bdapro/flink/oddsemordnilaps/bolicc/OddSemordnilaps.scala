package de.tu_berlin.dima.bdapro.flink.oddsemordnilaps.bolicc


import org.apache.flink.api.scala._
/**
  * Created by libo on 2016/10/26.
  */
object OddSemordnilaps {
  def reword(original:String)= List(original,original.reverse)
  def IsOdd(num:String)= !(num.toInt %2==0)
  def main(args: Array[String]) = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputPath=args(0)
    val text=env.readTextFile(inputPath)
      .flatMap(_.split("\\s+")).distinct().filter(x=>IsOdd(x))

    val rword =text.flatMap(x=>reword(x)).map((_,1)).groupBy(0).sum(1).filter(_._2==2)
      .sum(1).map(x=>x._2/2)

    rword.print()


  }


  //val semordnilap=text.filter()



}
