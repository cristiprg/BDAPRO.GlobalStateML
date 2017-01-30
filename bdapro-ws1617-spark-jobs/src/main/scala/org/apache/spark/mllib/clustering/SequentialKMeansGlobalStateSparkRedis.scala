package org.apache.spark.mllib.clustering

import java.io._

import de.tu_berlin.dima.bdapro.spark.global_state_api.StateManager

//import de.tu_berlin.dima.bdapro.spark.global_state_api.StateManager
import com.redislabs.provider.redis._
import de.tu_berlin.dima.bdapro.spark.mlalg.util.CSVFileSource
import org.apache.spark._
import org.apache.spark.mllib.linalg._
import org.apache.spark.streaming._
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Inspired from the example from databricks
  * https://docs.cloud.databricks.com/docs/spark/1.6/examples/Streaming%20mapWithState.html
  */
object SequentialKMeansGlobalStateSparkRedis {

  val sc= new SparkContext(new SparkConf()
    .setAppName("bdapro-globalstate-SequentialKMeansGlobalState")
    .setMaster("local[*]")
    .set("redis.host", "localhost") // initial redis host - can be any node in cluster
    .set("redis.port", "6379")) // initial redis port


  val pool: JedisPool = new JedisPool(new JedisPoolConfig(), "localhost")
  //val stateManager = new StateManager(sc)
  //val stateManager = new StateManager(pool)

  // === Configuration to control the flow of the application ===
  val stopActiveContext = true
  // "true"  = stop if any existing StreamingContext is running;
  // "false" = dont stop, and let it run undisturbed, but your latest code may not be used

  // === Configurations for Spark Streaming ===
  val batchIntervalSeconds = 1
  val eventsPerSecond = 2    // For the dummy source

  //val initialRDD = sc.parallelize(List(("1", 100L), ("2", 32L)))
  val identityMatrix: Matrix = Matrices.dense(3, 3, Array(1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0))
  val twoMatrix: Matrix = Matrices.dense(3, 3, Array(2.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0, 2.0))
 // val initialRDD = sc.parallelize(List((1, stateManager)))
  val stateSpec = StateSpec.function(trackStateFunc _)
    //.initialState(initialRDD)
//    .numPartitions(2)
    .timeout(Seconds(60))

  var newContextCreated = false      // Flag to detect whether new context was created or not

  /**
    * In this example:
    * - key is the word.
    * - value is '1'. Its type is 'Int'.
    * - state has the running count of the word. It's type is Long. The user can provide more custom classes as type too.
    * - The return value is the new (key, value) pair where value is the updated count.
    */
  def trackStateFunc(batchTime: Time, key: Int, value: Option[Int], pseudoState: State[StateManager]): Option[Matrix] = {
    //val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
    //val output = (key, sum)
    //state.update(sum)
    //Some(output)

    // Either take the matrix from the state or from Redis
    // Varianta cu State[Matrix]
    /*var stateMatrix = state.getOption.getOrElse(identityMatrix)//.getStateLocalMatrix()
    stateMatrix = stateMatrix.multiply(twoMatrix.asInstanceOf[DenseMatrix])
    state.update(stateMatrix)
    stateManager.setState(stateMatrix)
    Some(stateMatrix)*/

    // Varianta cu State[StateManager]
    val stateManager: StateManager = pseudoState.get()
    val stateMatrix = stateManager.getStateLocalMatrix()
    val newStateMatrix = stateMatrix.multiply(twoMatrix.asInstanceOf[DenseMatrix])
    stateManager.setState(newStateMatrix)
    Some(newStateMatrix)
  }


  //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //import sqlContext.implicits._

  // Function to create a new StreamingContext and set it up
  def creatingFunc(): StreamingContext = {
    val filePath = "/home/cristiprg/Spark/data/iris-small-2D.csv"
    // Create a StreamingContext

    val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))
    val dataStream = ssc.socketTextStream("localhost", 9999).map(Vectors.parse)
    //val dataStream = ssc.receiverStream(new CSVFileSource(1, filePath)).map(Vectors.parse)

    //sc.toRedisLIST(sc.parallelize(List("")), "evenOdd")
    //dataStream.foreachRDD(rdd => {

    //})




    val k = 4
    val trainingDataStream = ssc.receiverStream(new CSVFileSource(1, filePath)).map(Vectors.parse)
    //val trainingDataStream = ssc.socketTextStream("localhost", 9999).map(Vectors.parse)

    // Initialize the centroids and n_i = 0, for all i
    val initialState = Array(Seq("5.0", "5.0"), Seq("4.5", "3.5"),
      Seq("5.5", "3.5"), Seq("5.", "2.5"))
    val nrPointsInCluster = List("0", "0", "0", "0")

    // Save initial state in Redis
    var i = 0
    initialState.foreach(x => { // each centroid as a list
      sc.toRedisLIST(sc.parallelize(x), "centroid_" + i)
      i += 1
    })
    sc.toRedisLIST(sc.parallelize(nrPointsInCluster), "nrPointsInCluster")

    dataStream.map (x => {
      // 1. Retrieve the centroids and n_i
      var centroids: Array[Array[Double]] = Array()
      for( i <- 1 to k) {
        val centroid = sc.fromRedisList("centroid_" + i).collect()
        centroids = centroids :+ centroid.map(_.toDouble)
      }

      val nrPointsInCluster = sc.fromRedisList("nrPointsInCluster").collect().map(_.toDouble)

      // 2. Perform computations
      // ...

      // 3. Update centroids and n_i

      // Here is the issue:
      // I cannot update the lists, I can only append to the existing lists
      var i = 0
      centroids.foreach(c => { // each centroid as a list
        sc.toRedisLIST(sc.parallelize(c).map(_.toString), "centroid_" + i)
        i += 1
      })
      sc.toRedisLIST(sc.parallelize(nrPointsInCluster).map(_.toString), "nrPointsInCluster")
    })
    .print()

/*
    val wordStream = trainingDataStream.map (x => {
      //val stateMatrix = stateManager.getStateArrayOfVectors()
      //val nrPointsInCluster: DenseVector = stateManager.getStateLocalVector("n")
      val nrPointsInCluster = sc.fromRedisList("n").collect().map(_.toDouble)
      val stateMatrix: Array[Vector] = Array(Vectors.dense(5.0, 5.0), Vectors.dense(4.5, 3.5),
        Vectors.dense(5.5, 3.5), Vectors.dense(5.0, 2.5))

      // Compute the Euclidean Distance and take the minimum
      var closestCentroid: Vector = null
      var minDistance: Double = 9999999
      var index: Int = 0
      var closestCentroidIndex: Int = 0

      stateMatrix.foreach(centroid => {
        val copyCentroid: Vector = centroid.copy
        BLAS.axpy(-1, x, copyCentroid)
        val distance = Vectors.norm(copyCentroid, 2)

        if (distance < minDistance) {
          minDistance = distance
          closestCentroid = centroid
          closestCentroidIndex = index
        }
        index += 1
      })

      // n_i++
     // nrPointsInCluster.values(closestCentroidIndex) += 1
      nrPointsInCluster(closestCentroidIndex) += 1


      // Update the centroid
      var xMinusM = x
      BLAS.axpy(-1.0, closestCentroid, xMinusM)
      //BLAS.scal(1.0/nrPointsInCluster.values(closestCentroidIndex), xMinusM)
      BLAS.scal(1.0/nrPointsInCluster(closestCentroidIndex).toDouble, xMinusM)
      BLAS.axpy(1.0, xMinusM, closestCentroid)

      //stateManager.setState(stateMatrix)
      //stateManager.setStateLocalVector("n", nrPointsInCluster)

      sc.toRedisFixedLIST(sc.parallelize(nrPointsInCluster).map(_.toString), "n")

      closestCentroid
    })
    wordStream.print()
*/
    // This represents the emitted stream from the trackStateFunc. Since we emit every input record with the updated value,
    // this stream will contain the same # of records as the input dstream.
    // val wordCountStateStream = wordStream.mapWithState(stateSpec)
    // wordCountStateStream.print()

    // A snapshot of the state for the current batch. This dstream contains one entry per key.
    /*val stateSnapshotStream = wordStream.stateSnapshots()
    stateSnapshotStream.foreachRDD { rdd =>
    //  //rdd.toDF("word", "count").registerTempTable("batch_word_count")
      rdd.saveAsTextFile("/home/cristiprg/Spark/Output")
    }
  */
    ssc.remember(Minutes(1))  // To make sure data is not deleted by the time we query it interactively

    ssc.checkpoint("/home/cristiprg/Spark/Checkpoint")

    println("Creating function called to create new StreamingContext")
    newContextCreated = true
    ssc
  }

  def sendJSONs(): Unit = {
    // 1. Set up the monitoring thread
   /* new Thread("Cluster Centers Thread") {

      override def run(): Unit = {
        while(true) {
          val state = stateManager.getStateArrayOfVectors()
          val writer = new PrintWriter(new File("/home/cristiprg/Spark/data/centroids.json.js" ))
          var centroidSep = ""
          var coordSep = ""

          writer.write("data = [")
          state.foreach(centroid => {
            writer.write(centroidSep + '[')
            centroidSep = ","
            coordSep = ""
            centroid.foreachActive((index, value) => {

              writer.write(coordSep + value.toString)
              coordSep = ","
            })
            writer.write(']')
          })
          writer.write(']')
          writer.close()
          Thread.sleep(1000L)
        }
      }
    }//.start()*/
  }

  def main(args: Array[String]): Unit ={
    // Stop any existing StreamingContext
    if (stopActiveContext) {
      StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
    }

    // Get or create a streaming context
    val ssc = StreamingContext.getActiveOrCreate(creatingFunc)
    if (newContextCreated) {
      println("New context created from currently defined creating function")
    } else {
      println("Existing context running or recovered from checkpoint, may not be running currently defined creating function")
    }

    // Start the streaming context in the background.
    ssc.start()

    sendJSONs()

    // This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 2 * 1000)
  }


}
