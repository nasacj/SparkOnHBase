package com.nasacj.spark.sparkutils

import java.text.SimpleDateFormat
import java.util.Date

import com.nasacj.spark.hbase.HBaseSecurity
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableInputFormat}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.task.{JobContextImpl, TaskAttemptContextImpl}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast

class HBaseResultScanRDD (sc: SparkContext,
                          @transient tableName: String,
                          @transient scan: Scan,
                          configBroadcast: Broadcast[SerializableWritable[Configuration]],
                          credentialsConf: Broadcast[SerializableWritable[Credentials]])
  extends RDD[Result](sc, Nil)
  with LoggingExtended {
  @transient val jobTransient = new Job(configBroadcast.value.value, "ExampleRead");
  HBaseSecurity.initTableMapperJob(
    tableName, // input HBase table name
    scan, // Scan instance to control CF and attribute selection
    classOf[IdentityTableMapper], // mapper
    null, // mapper output key
    null, // mapper output value
    jobTransient);

  @transient val jobConfigurationTrans = jobTransient.getConfiguration()
  jobConfigurationTrans.set(TableInputFormat.INPUT_TABLE, tableName)

  val jobConfigBroadcast = sc.broadcast(new SerializableWritable(jobConfigurationTrans))
  ////

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  @transient protected val jobId = new JobID(jobTrackerId, id)

  override def getPartitions: Array[Partition] = {

    addCreds

    val tableInputFormat = new TableInputFormat
    tableInputFormat.setConf(jobConfigBroadcast.value.value)
    val jobContext = new JobContextImpl(jobConfigBroadcast.value.value, jobId)
    val rawSplits = tableInputFormat.getSplits(jobContext).toArray
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }

    result
  }

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[Result] = {

    addCreds

    val iter = new Iterator[Result] {

      addCreds

      val split = theSplit.asInstanceOf[NewHadoopPartition]
      logInfo("Input split: " + split.serializableHadoopSplit)
      val conf = jobConfigBroadcast.value.value

      val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
      val format = new TableInputFormat
      format.setConf(conf)

      val reader = format.createRecordReader(
        split.serializableHadoopSplit.value, hadoopAttemptContext)
      reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener(context => close())
      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          if (finished) {
            // Close and release the reader here; close() will also be called when the task
            // completes, but for tasks that read from many files, it helps to release the
            // resources early.
            close()
          }
          havePair = !finished
        }
        !finished
      }

      override def next(): Result = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false

        val result = reader.getCurrentValue

        result
      }

      private def close() {
        try {
          reader.close()
        } catch {
          case e: Exception => logWarning("Exception in RecordReader.close()", e)
        }
      }
    }
    new InterruptibleIterator(context, iter)
  }

  def addCreds {
    val creds = SparkHadoopUtil.get.getCurrentUserCredentials()
    if(creds != null){
      val ugi = UserGroupInformation.getCurrentUser();
      ugi.addCredentials(creds)
      // specify that this is a proxy user
      ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)
      ugi.addCredentials(credentialsConf.value.value)
    }
  }
}
