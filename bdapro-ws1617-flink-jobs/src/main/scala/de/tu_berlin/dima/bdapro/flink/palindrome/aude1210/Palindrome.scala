package de.tu_berlin.dima.bdapro.flink.palindrome.aude1210
import org.apache.flink.api.scala._
/**
  * Created by Aude on 25/10/2016.
  */
object Palindrome {

  def findPalindrome(sentence:String, start:Int, end :Int):Int = {
    if (start==end || end-start==1 || end == -1) sentence.length
    else{
      if (sentence.charAt(start)== sentence.charAt(end)) findPalindrome(sentence,start+1,end-1)
      else 0
    }
  }

  def main(args: Array[String]) {
    if (args.length != 1) {
      System.exit(-1)
    }
    val inputPath = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath).flatMap(_.split("\n")).map(x =>(x,findPalindrome(x.replaceAll(" ",""),0,x.replaceAll(" ","").length-1)))
    val len = text.max(1).map(_._2).collect()(0)
    text.filter(_._2 == len).map("The biggest palindrome sentence: <"+_._1+">").print()
  }
}