package QueryPac

import java.io.PrintWriter
import java.net.{ServerSocket, Socket}
import java.util.Scanner

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer


/**
 * Created by yang on 15-12-15.
 */
object QuerySlideWindow {
  val carContinuousTrajectoryPort: Int = 9999
  var flag: Int = -1
  //-1 means waiting to get a car id to query
  var carID: Int = -1
  //var trajBuff = ArrayBuffer[queryTrajDataFormat]()

  def main(args: Array[String]): Unit = {

    println("start listenning!!!!!!!!")

    val conf = new SparkConf().setMaster("local[4]").setAppName("DataReceiver")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val trajPoints = ssc.socketTextStream("localhost", 19999, StorageLevel.MEMORY_AND_DISK_SER)
    //设置窗口数据大小以及查询间隔

    val windowData = trajPoints.window(Seconds(100), Seconds(20))
    //每一秒的数据作为一个 batch,每个数据以时间为key,记录内容为value
    val record = windowData.map(line => {
      val temp = line.split("\t")
      if (temp.length != 2)
        throw new IllegalArgumentException("read data:" + temp + " error, because its elements is not two")
      val attrs = temp(1).split(",")
      val carID = attrs(0).toInt
      val longitude = attrs(1).toFloat
      val latitude = attrs(2).toFloat
      val speed = attrs(3).toFloat.toInt
      //将每条数据添加到trajBuff
      //trajBuff.append(new queryTrajDataFormat(temp(0).toLong, carID, longitude, latitude, speed))
      (temp(0), carID, longitude, latitude, speed)
    })


    //Query by CarID
    val arr = Array(9997, 9566, 5116, 10909, 12211)
    for (i <- 0 until arr.length) {
      carID = arr(i)
      val broadID = sc.broadcast(carID)
      println("query object id: " + carID)
      trajOfMOid(broadID, record, sc)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  class QueryReceiver extends Runnable {
    override def run(): Unit = {
      val socket = new Socket("127.0.0.1", carContinuousTrajectoryPort)
      try {
        println("begin to receive query")
        val inputStream = socket.getInputStream
        val in = new Scanner(inputStream)
        while (in.hasNextLine) {
          carID = in.nextLine().toInt
          println(carID)
        }
      } finally {
        socket.close()
      }
    }
  }


  def carContinuousTrajectoryPortListen(): Unit = {
    try {
      val server = new ServerSocket(carContinuousTrajectoryPort)
      val incoming = server.accept()
      try {
        val inStream = incoming.getInputStream
        val in = new Scanner(inStream)
        var done = false
        while (!done && in.hasNext()) {

          val line = in.nextLine()
          println(line)
          if (line.trim.equals("-1"))
            done = true
        }
      } finally {
        incoming.close()
      }
    } catch {
      case ex: Exception => println("carContinuousTra数据大小以及查询间隔jectoryPortListen Exception")
    }
  }

  //给定对象查询连续汇报其轨迹
  def trajOfMOid(MoIDBrodcast: Broadcast[Int], trajPoints: DStream[(String, Int, Float, Float, Int)],
                 sc: SparkContext): Unit = {
    val qResult = trajPoints.filter(x => x._2 == MoIDBrodcast.value)
    println("query result of MoID: " + MoIDBrodcast.value)
    qResult.print()

  }

  //查询
/*  def windowDataQuery(traj: ArrayBuffer[queryTrajDataFormat], sc: SparkContext): Unit = {
    val queryMbr = new queryByMBR(116.392086, 39.907109, 116.414651, 39.920944)
    val carIDsFromMBR = queryMbr.getAllCarIDs(traj, queryMbr)
    val highSpeedCars = queryMbr.getHighSpeedCars(30, traj, queryMbr)
    println("All Cars: ")
    if(!carIDsFromMBR.isEmpty){
      for(i <- 0 until carIDsFromMBR.length){
        print(carIDsFromMBR(i) + "  ")
      }
    }

    println("highSpeedCars: ")
    if(!highSpeedCars.isEmpty){
      for(j <- 0 until highSpeedCars.length){
        print(highSpeedCars(j) + "  ")
      }
    }
  }*/

  def windowDataQuery(traj: DStream[(String, Int, Float, Float, Int)] , sc: SparkContext): Unit = {
    val queryMbr = new queryByMBR(116.392086, 39.907109, 116.414651, 39.920944)
    val trajBuff = new ArrayBuffer[queryTrajDataFormat]()
    val record = traj.map(line => {
      val time = line._1
      val carID = line._2.toInt
      val longitude = line._3.toDouble
      val latitude = line._4.toDouble
      val speed = line._5.toInt
      //将每条数据添加到trajBuff
      trajBuff.append(new queryTrajDataFormat(time.toInt, carID, longitude, latitude, speed))
      (time, carID, longitude, latitude, speed)
    })
    val carIDsFromMBR = queryMbr.getAllCarIDs(trajBuff, queryMbr)
    val highSpeedCars = queryMbr.getHighSpeedCars(40, trajBuff, queryMbr)
    println("All Cars: ")
    if(!carIDsFromMBR.isEmpty){
      for(i <- 0 until carIDsFromMBR.length){
        print(carIDsFromMBR(i) + "  ")
      }
    }

    println("highSpeedCars: ")
    if(!highSpeedCars.isEmpty){
      for(j <- 0 until highSpeedCars.length){
        print(highSpeedCars(j) + "  ")
      }
    }
  }


  //获得grid Index from file
  def getSpatialIndex(spatialIndexFile: String): Unit = {}
  //查询当前窗口内 给定OID的轨迹
  def oidWindow(dStream: DStream[(String, Int, Float, Float, Int)], windowLength: Int, slidingInterval: Int): Unit = {}


}

