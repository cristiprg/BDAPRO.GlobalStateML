package de.tu_berlin.dima.bdapro.flink.palindrome.bolicc

/**
  * Created by osboxes on 01/11/16.
  */
import org.apache.flink.api.scala._

object Palindrome {
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputPath=args(0)
    val text=env.readTextFile(inputPath)

    def isPalindrome(someNumber:String):Boolean = {
      {if(someNumber==someNumber.reverse)
        return true
      }
      return false
    }





    val palindromeword=text.map(x=>(x,x.trim().split("\\s+").mkString("")))
    val palindromelen=palindromeword.map{x=>{if(isPalindrome(x._2)) {(x._2.length(),x._1)}else{(0,x._1)}}}
    //.groupBy(0) the group results are wrong.
    val maxva=  palindromelen.max(0) //somethingwrong
    //maxva.print()
    //how to convert Dataset[int] to Int?
    val outputp =palindromelen.join(maxva).where(0).equalTo(0)
      .map(x=>"The biggest palindrome sentence: <"+x._1._2+">")
    outputp.print()

  }

}



