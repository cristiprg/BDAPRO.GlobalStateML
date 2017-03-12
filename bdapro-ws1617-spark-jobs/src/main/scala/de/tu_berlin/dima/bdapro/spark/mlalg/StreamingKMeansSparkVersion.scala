package de.tu_berlin.dima.bdapro.spark.mlalg

import de.tu_berlin.dima.bdapro.spark.mlalg.util.CSVFileSource
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.{StreamingKMeans, StreamingKMeansGlobalState}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by cristiprg on 8-1-17.
  */
object StreamingKMeansSparkVersion {

  val batchIntervalSeconds = 1
  val monitoringIntervalSeconds = 1
  var filePath = ""
  val numberOfClusters = 4

  // Global variable across threads
  var modelConfig: StreamingKMeans = null

  def creatingFunc(): StreamingContext = {

    val conf = new SparkConf()
      .setAppName("StreamingKMeansSparkVersion")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(batchIntervalSeconds))
    val initialState: Array[Vector] = Array(Vectors.dense(0.0, 0.0, 0.0, 0.0), Vectors.dense(1.0, 1.0, 1.0, 1.0),
      Vectors.dense(2.0, 2.0, 2.0, 2.0), Vectors.dense(3.0, 3.0, 3.0, 3.0))

    val trainingDataStream = ssc.receiverStream(new CSVFileSource(100, "/home/cristiprg/Spark/iris6.csv" )).map(Vectors.parse)
    //val trainingDataStream = ssc.textFileStream(filePath).flatMap(_.split("\n")).map('[' + _ + ']').map(Vectors.parse)

    //trainingDataStream.print()
    modelConfig = new StreamingKMeans()
      .setK(numberOfClusters)
      .setDecayFactor(0.9)
      .setInitialCenters(initialState, Array(1, 1, 1, 1))
      //.setRandomCenters(4, 1)

    modelConfig.trainOn(trainingDataStream)


    //modelConfig.latestModel().clusterCenters.foreach(println)
    //modelConfig.predictOn(testDataStream).print()

    ssc
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("Usage: <jar> inputPath")
      System.exit(-1)
    }

    filePath = args(0)

    // 1. Set up the monitoring thread
    new Thread("Cluster Centers Thread") {
      override def run(): Unit = {
        while(true) {
          if (modelConfig != null)
            modelConfig.latestModel().clusterCenters.foreach(print)
            println
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
