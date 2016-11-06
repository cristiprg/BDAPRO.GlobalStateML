package de.tu_berlin.dima.bdapro.flink.palindrome.serkanali

import org.apache.flink.api.scala._

/**
  * Created by nerka on 31.10.2016.
  */
object Palindrome {
  def main(args: Array[String]): Unit = {
    if(args.length < 1)
      System.exit(-1)

    val inputPath = args(0)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val allSentences =  env.readTextFile(inputPath)
                        .flatMap(_.toLowerCase.split("\n"))

    val lenghtSetences =  allSentences
                            .map(value =>(value, value.replace(" ","").replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", "").length()))
                            .filter(value => FindPalindrome(value._1.replace(" ","").replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", ""),value._2))
    val MaxLength = lenghtSetences
                      .max(1)

    lenghtSetences
        .join(MaxLength)
        .where(1)
        .equalTo(1){
          (lenghtSetences, MaxLength) => ("The longest Sentence: <" +lenghtSetences._1 + ">")
        }
        .print()
  }

  def FindPalindrome(sentence: String, senLenght: Int): Boolean ={
    var indexB = 0
    if(senLenght == 0)
      return true
    var indexE = senLenght - 1
    while( sentence.charAt(indexB).equals(sentence.charAt(indexE))) {
      indexB = indexB + 1
      indexE = indexE - 1
      if (indexB >= indexE )
        return true
    }
    return false
  }
}

