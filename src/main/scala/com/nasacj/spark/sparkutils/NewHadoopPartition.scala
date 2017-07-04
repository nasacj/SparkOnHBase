package com.nasacj.spark.sparkutils

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.{SerializableWritable, Partition}

class NewHadoopPartition (
                           rddId: Int,
                           val index: Int,
                           @transient rawSplit: InputSplit with Writable)
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable(rawSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + index
}
