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
    val windowData = trajPoints.window(Seconds(10), Seconds(1))
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



      val messageDataFrame = message.map(x => TaxiMeesage(0,x._1,x._2,x._3,x._4,x._5)).toDF()
      messageDataFrame.registerTempTable("TaxiMessages")
      sqlcontext.cacheTable("TaxiMessages")

      //total car numbers
      /*val messageCountDF = sqlcontext.sql("select count(id) as totalNum from TaxiMessages " +
        " where speed > 80 and longitude > 116.2 and longitude < 116.5 and latitude > 39.85 and latitude < 40.1")
      messageCountDF.show()*/

      //count every Car's traj number
      val CarTrajCount = sqlcontext.sql("select id, count(id) as total from TaxiMessages " +
        "where longitude > 116.2 and longitude < 116.5 and latitude > 39.85 and latitude < 40.1 " +
        "group by id having(total>0) order by total desc")
      CarTrajCount.show(100)

      //all car id
      /*val AllCarIDsDF = sqlcontext.sql("select distinct id as CarID from TaxiMessages " +
        "where speed > 80 and longitude > 116.2 and longitude < 116.5 and latitude > 39.85 and latitude < 40.1")
      AllCarIDsDF.show()*/

      //Query All Car's Speed Desc
      val AvgSpeedTableDF = sqlcontext.sql("select id, avg(speed) as AvgSpeed from TaxiMessages " +
        "where speed > 80 and longitude > 116.2 and longitude < 116.5 and latitude > 39.85 and latitude < 40.1" +
        " group by id having(AvgSpeed>0) order by AvgSpeed desc")
      AvgSpeedTableDF.show(100)

      val AllCarAvgSpeed = sqlcontext.sql("select avg(AvgSpeed) as AvgSpeedOfArea max(AvgSpeed) as MaxSpeed" +
        " min(AvgSpeed) as MinSpeed from AvgSpeedTableDF")
      AllCarAvgSpeed.show()


      //缓存数组方法
      println("---------------------------------------------")
      val queryByMbr = new queryByMBR(116.232656,39.869983,116.544261,40.021769)
      val queryBuff = new ArrayBuffer[queryTrajDataFormat]()
      val singleLine = message.map(line => {
        val time = line._1.toInt
        val carID = line._2.toInt
        val longitude = line._3.toFloat
        val latitude = line._4.toFloat
        val speed = line._5.toFloat
        queryBuff.append(new queryTrajDataFormat(time, carID, longitude, latitude, speed))
        (time, carID, longitude, latitude, speed)
      })
      //val queryDataFrame = message.map(x => queryBuff.append(new queryTrajDataFormat(x._1.toInt,x._2.toInt,x._3.toFloat,x._4.toFloat,x._5.toFloat)))
      println("Average Speed:" + queryByMbr.getAllCarsAverageSpeed(queryBuff, queryByMbr))
      val highSpeed: ArrayBuffer[Double] = (queryByMbr.getHighSpeedCars(80, queryBuff, queryByMbr))
      for (i <- 0 until highSpeed.length){
        println(highSpeed(i))
      }
    })


    record.foreachRDD(rdd=>{
      rdd.foreachPartition(partition =>{ println( partition.mkString(","))
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