package de.tu_berlin.dima.bdapro.spark.mlalg

import de.tu_berlin.dima.bdapro.spark.global_state_api.StateManager
import de.tu_berlin.dima.bdapro.spark.mlalg.util.DummySource
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._

import org.apache.spark.mllib.linalg.{DenseMatrix, Vectors, Matrices, Matrix}

/**
  * Inspired from the example from databricks
  * https://docs.cloud.databricks.com/docs/spark/1.6/examples/Streaming%20mapWithState.html
  */
object MultiplyMatrixWithStateManager {

  val sc= new SparkContext(new SparkConf()
    .setAppName("bdapro-globalstate-wordCountWithState")
    .setMaster("local"))

  val stateManager = new StateManager(sc)

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
  val initialRDD = sc.parallelize(List((1, identityMatrix)))
  val stateSpec = StateSpec.function(trackStateFunc _)
    .initialState(initialRDD)
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
  def trackStateFunc(batchTime: Time, key: Int, value: Option[Int], state: State[Matrix]): Option[Matrix] = {
    //val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
    //val output = (key, sum)
    //state.update(sum)
    //Some(output)

    // Either take the matrix from the state or from Redis
    var stateMatrix = state.getOption.getOrElse(identityMatrix)//.getStateLocalMatrix()
    stateMatrix = stateMatrix.multiply(twoMatrix.asInstanceOf[DenseMatrix])
    state.update(stateMatrix)
    stateManager.setState(stateMatrix)
    Some(stateMatrix)
  }


  //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //import sqlContext.implicits._

  // Function to create a new StreamingContext and set it up
  def creatingFunc(): StreamingContext = {

    // Create a StreamingContext
    val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))

    // Create a stream that generates 1000 lines per second
    val stream = ssc.receiverStream(new DummySource(eventsPerSecond))

    // Split the lines into words, and create a paired (key-value) dstream
    // val wordStream = stream.flatMap { _.split(" ")  }.map(word => (word, 1))
    val wordStream = stream.map ( x => (x,x) ).mapWithState(stateSpec)
    wordStream.print()

    // This represents the emitted stream from the trackStateFunc. Since we emit every input record with the updated value,
    // this stream will contain the same # of records as the input dstream.
    // val wordCountStateStream = wordStream.mapWithState(stateSpec)
    // wordCountStateStream.print()

    // A snapshot of the state for the current batch. This dstream contains one entry per key.
    val stateSnapshotStream = wordStream.stateSnapshots()
    stateSnapshotStream.foreachRDD { rdd =>
    //  //rdd.toDF("word", "count").registerTempTable("batch_word_count")
      rdd.saveAsTextFile("/home/cristiprg/Spark/Output")
    }

    ssc.remember(Minutes(1))  // To make sure data is not deleted by the time we query it interactively

    ssc.checkpoint("/home/cristiprg/Spark/Checkpoint")

    println("Creating function called to create new StreamingContext")
    newContextCreated = true
    ssc
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

    // This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 2 * 1000)
  }


}
