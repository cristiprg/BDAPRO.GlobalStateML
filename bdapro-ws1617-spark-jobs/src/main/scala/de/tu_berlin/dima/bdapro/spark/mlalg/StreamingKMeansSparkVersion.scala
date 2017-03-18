package de.tu_berlin.dima.bdapro.spark.mlalg

import de.tu_berlin.dima.bdapro.spark.mlalg.util.CSVFileSource
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.{StreamingKMeans, StreamingKMeansGlobalState}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

/**
  * Created by cristiprg on 8-1-17.
  */
object StreamingKMeansSparkVersion {

  var batchIntervalMilliseconds: Long = 1
  val monitoringIntervalSeconds = 1
  var redisServerAddress = ""
  val numberOfClusters = 4
  var textSocketStreamAddress: String = ""

  var whichKMeans: String = ""

  var modelConfigGlobalState: StreamingKMeansGlobalState = null
  var modelConfig: StreamingKMeans = null

  def creatingFunc(): StreamingContext = {

    val conf = new SparkConf()
      .setAppName("StreamingKMeansSparkVersion")
        //.setMaster("local[*]")

    val ssc = new StreamingContext(conf, Milliseconds(batchIntervalMilliseconds))
    val initialState: Array[Vector] = Array(Vectors.dense(4,5,3,5), Vectors.dense(5,5,5,4),
      Vectors.dense(5,5,4,5), Vectors.dense(5,4,5,5))

    //val trainingDataStream = ssc.receiverStream(new CSVFileSource(100, "/home/cristiprg/Spark/iris6.csv" )).map(Vectors.parse)
    //val trainingDataStream = ssc.textFileStream(filePath).flatMap(_.split("\n")).map('[' + _ + ']').map(Vectors.parse)
    val trainingDataStream = ssc.socketTextStream(textSocketStreamAddress, 9999).map('[' + _ + ']').map(Vectors.parse)

    //trainingDataStream.print()
    whichKMeans match {
      case "ORIGINAL" =>
        modelConfig = new StreamingKMeans()
          .setK(numberOfClusters)
          .setDecayFactor(0.9)
          .setInitialCenters(initialState, Array(1, 1, 1, 1))
        //.setRandomCenters(4, 1)
        modelConfig.trainOn(trainingDataStream)

      case "GLOBALSTATE" =>
        modelConfigGlobalState = new StreamingKMeansGlobalState(redisServerAddress)
        .setK(numberOfClusters)
        .setDecayFactor(0.9)
        .setInitialCenters(initialState, Array(1, 1, 1, 1))
        //.setRandomCenters(4, 1)

        modelConfigGlobalState.trainOn(trainingDataStream)
    }



    //modelConfig.latestModel().clusterCenters.foreach(println)
    //modelConfig.predictOn(testDataStream).print()

    ssc
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println("Usage: <jar> redisServerAddress batchIntervalMilliseconds textSocketStreamAddress <ORIGINAL|GLOBALSTATE>")
      System.exit(-1)
    }

    redisServerAddress = args(0)
    batchIntervalMilliseconds = args(1).toLong
    textSocketStreamAddress = args(2)
    whichKMeans = args(3)

    // 1. Set up the monitoring thread
    new Thread("Cluster Centers Thread") {
      override def run(): Unit = {
        while(true) {
          if (modelConfigGlobalState != null) {modelConfigGlobalState.latestModel().clusterCenters.foreach(print);println}
          if (modelConfig != null) {modelConfig.latestModel().clusterCenters.foreach(print);println}

          Thread.sleep(monitoringIntervalSeconds * 1000L)
        }
      }
    }.start()

    // 2. Start Spark job
    val ssc = StreamingContext.getActiveOrCreate(creatingFunc)
    ssc.start()
    ssc.awaitTermination()
  }
}
