package com.nasacj.spark.sparkutils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HConnectionManager, Result, Scan}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class HbaseReceiver[T](config: Configuration, tableName: String, scan: Scan, f: (Result) => T, storageLevel: StorageLevel)
  extends Receiver[T](storageLevel) {
  def onStart() {
    // Setup stuff (start threads, open sockets, etc.) to start receiving data.
    // Must start new thread to receive data, as onStart() must be non-blocking.

    // Call store(...) in those threads to store received data into Spark's memory.

    // Call stop(...), restart(...) or reportError(...) on any thread based on how
    // different errors needs to be handled.

    // See corresponding method documentation for more details

    // Start the thread that receives data over a connection
    println("In HbaseReceiver onStart()")
    new Thread("Hbase Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {
    // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
    println("In HbaseReceiver onStop()")
  }

  private def receive() {
    println("In HbaseReceiver receive()")
    val hConnection = HConnectionManager.createConnection(config)
    val htable = hConnection.getTable(tableName)
    println("table=" + htable.getName)
    val scanner = htable.getScanner(scan)
    val scalaIter = scanner.iterator()
    println("has Next=" + scalaIter.hasNext())

    try {
      while (scalaIter.hasNext()) {
        val result = scalaIter.next()
        println("Result=" + result)
        println("Calling Store f(result)=" + f(result))
        store(f(result))
        println("has Next=" + scalaIter.hasNext())
      }
    } catch {
      case ex: Throwable => {
        println("Exception:" + ex)
      }
    }
    finally {
      println("Closing connection")
      scanner.close()
      htable.close()
      hConnection.close()
    }
  }

}
