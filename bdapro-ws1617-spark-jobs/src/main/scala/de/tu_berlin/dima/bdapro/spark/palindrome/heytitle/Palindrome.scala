package de.tu_berlin.dima.bdapro.spark.palindrome.heytitle

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by heytitle on 10/28/16.
  */
object Palindrome {
  val file = "file:///Users/heytitle/projects/BDAPRO.WS1617/bdapro-ws1617-bundle/src/main/resources/datasets/palindromeSmall.txt"

  val reducedWithCaseClass: (List[Sentence], List[Sentence] ) => List[Sentence] = (as, bs) => {
    (as(0).length,bs(0).length) match {
      case (a,b) if  a > b => as
      case (a,b) if  a < b => bs
      case _ => as ::: bs
    }
  }

  def main(args: Array[String]) {
    if (args.length != 1) {
      Console.err.println("Usage: <jar> inputPath")
    }

    val inputPath = if( args.length == 1 ) args(0) else file


    val spark = new SparkContext("local", "palindrome")
    val pars = spark.textFile(inputPath)
      .map( s => Sentence(s) )
      .filter( _.isPalindrome )
        .cache()

    val max = pars.map( _.length ).max()

    pars.filter( _.length == max ).foreach( println )

//    pars.foreach(println)

  }

  case class Sentence(s: String){
    private val ns = s.replaceAll(" ", "")
    val isPalindrome = ns.reverse == ns
    val length = ns.length

    override def toString: String = {
      return s"The biggest palindrome sentence: <$s>"
    }
  }

}
