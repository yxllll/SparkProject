package QueryPac

/**
 * Created by yang on 15-12-16.
 */
class temp {

  /*   val queryMbr = new queryByMBR(116.392086, 39.907109, 116.414651, 39.920944)
   val carIDsFromMBR = queryMbr.getAllCarIDs(trajBuff, queryMbr)
   val highSpeedCars = queryMbr.getHighSpeedCars(30, trajBuff, queryMbr)
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
   }*/
}


/*
package QueryPac

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Time, StreamingContext, Seconds}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by yang on 15-12-16.
 */
object SparkStreamingToHBase {
  def main(args: Array[String]): Unit = {

    println("start listenning!!!!!!!!")

    val conf = new SparkConf().setMaster("local[4]").setAppName("DataReceiver")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc,Seconds(10))

    val haaseconf  = new HBaseConfiguration()
    //  val hbaseContext = new hbaseContext

    val sqlcontext = new SQLContext(sc)


    val trajPoints = ssc.socketTextStream("localhost",19999,StorageLevel.MEMORY_AND_DISK_SER)
    //每一妙的数据作为一个 batch,每个数据以时间为key,记录内容为value
    val windowData = trajPoints.window(Seconds(200), Seconds(20))
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

    //val seqBatch =  new ArrayBuffer[RDD[(Int,Int,Float,Float,Int)]]()

    record.foreachRDD( (message:RDD[(Int,Int,Float,Float,Int)], time: Time)=>{
      //使用DataFrame进行查询
      import sqlcontext.implicits._

      val seqBatch = new ArrayBuffer[RDD[(Int,Int,Float,Float,Int)]]()
      //seqBatch.append(message)
      seqBatch.append(message)
      val messageDataFrame = message.map(x => TaxiMeesage(0,x._1,x._2,x._3,x._4,x._5)).toDF()
      messageDataFrame.registerTempTable("TaxiMessages")
      sqlcontext.cacheTable("TaxiMessages")
      /*val messageCountDF = sqlcontext.sql("select distinct id, count(id) as total from TaxiMessages where " +
        "id <100 group by id")
      messageCountDF.show(200)*/
      //total car numbers
      val messageCountDF = sqlcontext.sql("select count(distinct id) as totalNum from TaxiMessages " +
        " where speed > 80 and longitude > 116.2 and longitude < 116.5 and latitude > 39.85 and latitude < 40.1")
      messageCountDF.show()
      //all car id
      val AllCarIDsDF = sqlcontext.sql("select distinct id as CarID from TaxiMessages " +
        "where speed > 80 and longitude > 116.2 and longitude < 116.5 and latitude > 39.85 and latitude < 40.1")
      AllCarIDsDF.show()
      /* val speedDF = sqlcontext.sql("select * from TaxiMessages where speed > 80 and longitude > 116.2" +
         "and longitude < 116.5 and latitude > 39.85 and latitude < 40.1")
       speedDF.show(200)
       val AvgAndMaxAndMinSpeedDF = sqlcontext.sql("select avg(speed) as AvgSpeed, max(speed) as " +
         "MaxSpeed, min(speed) as MinSpeed from TaxiMessages ")
       AvgAndMaxAndMinSpeedDF.show()*/


      //Query All Car's Speed Desc
      val AvgSpeedTableDF = sqlcontext.sql("select id, avg(speed) as AvgSpeed from TaxiMessages " +
        "where speed > 80 and longitude > 116.2 and longitude < 116.5 and latitude > 39.85 and latitude < 40.1" +
        " group by id having(AvgSpeed>0) order by AvgSpeed desc")
      AvgSpeedTableDF.show(100)


      println("---------------------------------------------")
      for(i <- 0 until seqBatch.length){
        println(seqBatch(i))
      }

    })

    record.foreachRDD(rdd=>{
      rdd.foreachPartition(partition =>{

      })
    })
    ssc.start()
    ssc.awaitTermination()
    /* record.foreachRDD{rdd=>
       rdd.mapPartitions{ partition=>
         val tem = //链接数据库
         partition.foreach( record=> )

       }

     }*/
  }

  case class TaxiMeesage(region:Int,time:Long,id:Int,longitude:Float,latitude:Float,speed:Int)

  class HBaseConfiguration()

}
*/
