package QueryPac

import QueryPac.SparkStreamingToHBase.TaxiMeesage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by yang on 15-12-18.
 */
object test {
  def main(args: Array[String]): Unit = {

    println("start listenning!!!!!!!!")

    val conf = new SparkConf().setMaster("local[4]").setAppName("DataReceiver")
    val sc = new SparkContext(conf)



    val ssc = new StreamingContext(sc,Seconds(1))

    val hbaseconf  = new HBaseConfiguration()
    //  val hbaseContext = new hbaseContext

    val sqlcontext = new SQLContext(sc)


    val trajPoints = ssc.socketTextStream("localhost",19999,StorageLevel.MEMORY_AND_DISK_SER)
    //每一妙的数据作为一个 batch,每个数据以时间为key,记录内容为value

    val windowData = trajPoints.window(Seconds(8),Seconds(2))
    val record =  windowData.map(line=>{
      val temp = line.split("\t")
      if(temp.length!=2)
        throw new IllegalArgumentException("read data:"+temp+" error, because its elements is not two")

      val attrs = temp(1).split(",")
      val carID = attrs(0).toInt
      val longitude = attrs(1).toFloat
      val latitude = attrs(2).toFloat
      val speed = attrs(3).toFloat.toInt
      (temp(0).toInt,carID, longitude, latitude, speed)
    })
    //message :RDD[]
    // val windowData = trajPoints.window(Seconds(2), Seconds(2))
    //val seqBatch =  new ArrayBuffer[RDD[(Int,Int,Float,Float,Int)]]()
    val seqBatch =  new ArrayBuffer[RDD[(Int,Int,Float,Float,Int)]]()
    //val myRecords = new ArrayBuffer[(Int,Int,Float,Float,Int)]()


    record.foreachRDD( (message:RDD[(Int,Int,Float,Float,Int)], time: Time)=>{
      //使用DataFrame进行查询
      import sqlcontext.implicits._

      val b = message.cache()
      seqBatch.append(b)
   //   seqBatch.append(newMe)
   //   seqBatch.append(message)

      /*val messageDataFrame = message.map(x => TaxiMeesage(0,x._1,x._2,x._3,x._4,x._5)).toDF()
      messageDataFrame.registerTempTable("TaxiMessages")
      sqlcontext.cacheTable("TaxiMessages")

      //count every Car's traj number
      val CarTrajCount = sqlcontext.sql("select id, count(id) as total from TaxiMessages " +
        "where longitude > 116.2 and longitude < 116.5 and latitude > 39.85 and latitude < 40.1 " +
        "group by id having(total>0) order by total desc")
      CarTrajCount.show(100)


      //Query All Car's Speed Desc
      val AvgSpeedTableDF = sqlcontext.sql("select id, avg(speed) as AvgSpeed from TaxiMessages " +
        "where speed > 80 and longitude > 116.2 and longitude < 116.5 and latitude > 39.85 and latitude < 40.1" +
        " group by id having(AvgSpeed>0) order by AvgSpeed desc")
      AvgSpeedTableDF.show(100)*/


      //缓存数组方法
      println("---------------------------------------------")

      for(i <- 0 until seqBatch.length){
        println(seqBatch(i))
      //  seqBatch(i).foreach(x=>println(x))
          seqBatch(i).foreachPartition(partition =>{ println( partition.mkString(","))})
        println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      }

    })


/*    record.foreachRDD(rdd=>{
      rdd.foreachPartition(partition =>{
      })
    })*/
    ssc.start()
    ssc.awaitTermination()

  }

  case class TaxiMeesage(region:Int,time:Long,id:Int,longitude:Float,latitude:Float,speed:Int)

  class HBaseConfiguration()

}