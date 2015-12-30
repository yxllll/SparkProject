package QueryPac

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by yang on 15-12-29.
 */
object TestOnly {

  def main(args: Array[String]): Unit = {

    println("start listenning!!!!!!!!")
    val conf = new SparkConf().setMaster("local[4]").setAppName("DataReceiver")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val hbaseconf = new HBaseConfiguration()
    //  val hbaseContext = new hbaseContext
    val sqlcontext = new SQLContext(sc)

    val trajPoints = ssc.socketTextStream("localhost", 19999, StorageLevel.MEMORY_AND_DISK_SER)
    //每一妙的数据作为一个 batch,每个数据以时间为key,记录内容为value
    val windowData = trajPoints.window(Seconds(8), Seconds(2))
    val record = windowData.map(line => {
      val temp = line.split("\t")
      if (temp.length != 2)
        throw new IllegalArgumentException("read data:" + temp + " error, because its elements is not two")

      val attrs = temp(1).split(",")
      val carID = attrs(0).toInt
      val longitude = attrs(1).toFloat
      val latitude = attrs(2).toFloat
      val speed = attrs(3).toFloat.toInt
      (temp(0).toInt, carID, longitude, latitude, speed)
    })

    record.foreachRDD((message: RDD[(Int, Int, Float, Float, Int)], time: Time) => {

      val seqBatchin = new ArrayBuffer[RDD[(Int, Int, Float, Float, Int)]]()

      seqBatchin.append(message)
      println("---------------------------------------------")
      for (i <- 0 until seqBatchin.length) {
        println(seqBatchin(i))
        seqBatchin(i).foreach(x => println(x))
        seqBatchin(i).foreachPartition(partition => {
          println(partition.mkString(","))
        })
        println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

  case class TaxiMeesage(region: Int, time: Long, id: Int, longitude: Float, latitude: Float, speed: Int)

  class HBaseConfiguration()

}