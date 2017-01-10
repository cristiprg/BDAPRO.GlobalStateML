package de.tu_berlin.dima.bdapro.spark.mlalg.util

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.io.Source
import scala.util.Random

/**
  * This custom receiver reads a CSV file and sends one line at a time. For local use only.
  */
class CSVFileSource(ratePerSec: Int, filePath: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("CSVFileSourceThread") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Send line by line */
  private def receive() {
    val data = Source.fromFile(filePath)
      .getLines()
      .map('[' + _ + ']')
      .toSeq

    data.foreach( x => {
      store(x)
      Thread.sleep((1000.toDouble / ratePerSec).toInt)
    })
  }
}
