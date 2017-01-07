package de.tu_berlin.dima.bdapro.spark.mlalg.util

import org.apache.spark.storage.StorageLevel
import scala.util.Random
import org.apache.spark.streaming.receiver._

/**
  * This is taken from an example from databricks.
  * https://docs.cloud.databricks.com/docs/spark/1.6/examples/Streaming%20mapWithState.html
  * This is the dummy source implemented as a custom receiver.
  */
class DummySource(ratePerSec: Int) extends Receiver[Int](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Dummy Source") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    while(!isStopped()) {
      store(Random.nextInt(9) + 1 )
      Thread.sleep((1000.toDouble / ratePerSec).toInt)
    }
  }
}
