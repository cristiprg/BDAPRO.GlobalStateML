package de.tu_berlin.dima.bdapro.spark.mlalg

import de.tu_berlin.dima.bdapro.spark.mlalg.util.CSVFileSource
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by cristiprg on 8-1-17.
  */
object StreamingKMeansSparkVersion {

  val batchIntervalSeconds = 1
  val monitoringIntervalSeconds = 1
  val filePath = "/home/cristiprg/Spark/data/iris2.csv"
  val numberOfClusters = 2

  // Global variable across threads
  var modelConfig: StreamingKMeans = null

  def creatingFunc(): StreamingContext = {

    val conf = new SparkConf()
      .setAppName("StreamingKMeansSparkVersion")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(batchIntervalSeconds))

    val trainingDataStream = ssc.receiverStream(new CSVFileSource(1000, filePath)).map(Vectors.parse)

    modelConfig = new StreamingKMeans()
      .setK(numberOfClusters)
      .setDecayFactor(0.9)
//      .setInitialCenters(Array(Vectors.dense(7.3,2.9,6.3,1.8), Vectors.dense(5.1,3.5,1.4,0.2)), Array(1, 1))
      .setRandomCenters(4, 0)

    modelConfig.trainOn(trainingDataStream)

    //modelConfig.latestModel().clusterCenters.foreach(println)
    //modelConfig.predictOn(testDataStream).print()

    ssc
  }

  def main(args: Array[String]): Unit = {

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
