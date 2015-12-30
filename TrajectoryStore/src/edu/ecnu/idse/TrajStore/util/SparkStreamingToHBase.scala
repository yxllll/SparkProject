package edu.ecnu.idse.TrajStore.util

/*import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HConnectionManager*/

import edu.ecnu.idse.TrajStore.core.{CellInfo, SpatialTemporalSite}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SerializableWritable, SparkConf, SparkContext}

/**
  * Created by zzg on 15-12-15.
  */


class SparkStreamingToHBase {
  val down :Float = 39.26f
  val top:Float = 41.03f
  val left:Float = 115.25f
  val right:Float =117.30f
  val gridSize = 50
  val width =( right -left)/gridSize
  val height = (top - down)/gridSize


}

object SparkStreamingToHBase {
  final val tablename = "taxims"
  final val family = Bytes.toBytes("as")
  final  val c_speed = Bytes.toBytes("s")
  final  val c_log = Bytes.toBytes("lo")
  final  val c_lat = Bytes.toBytes("la")

  def main(args: Array[String]): Unit = {

    val hBaseConf  = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum","localhost")//指定ip地址
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181") // zookeeper的端口号


    val SaveJobConf:JobConf  = new JobConf(hBaseConf,this.getClass)
    SaveJobConf.set("mapreduce.output.fileoutputformat.outputdir","hdfs://localhost:9000/user/zzg/table")
    SaveJobConf.setOutputFormat(classOf[TableOutputFormat])
    SaveJobConf.set(TableOutputFormat.OUTPUT_TABLE,tablename)

    val conf = new SparkConf().setMaster("local[4]").setAppName("DataReceiver")
    conf.set("spark.serializer", "org,apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[CellInfo], classOf[Text]))

    val sc = new SparkContext(conf)

    println("reading indexes" + SaveJobConf)
    val indexs = SpatialTemporalSite.ReadSpatialIndex(SaveJobConf)
    for(i<-0 until  indexs.length){
      println(i+"\t"+indexs(i).cellId+"\t"+ indexs(i).x1+" "+indexs(i).y1+"\t"+indexs(i).x2+" "+indexs(i).y2)
    }

    //   val bcIndexes = sc.broadcast(new SerializableWritable(indexs))
     val bcIndexes = sc.broadcast(indexs)
 /*   val hAdmin = new HBaseAdmin(hBaseConf)
    println("creating table")
    val messageTable = createTable(hAdmin,"TaxiMessages")
*/

    val ssc = new StreamingContext(sc,Seconds(5))

    //  val hbaseContext = new hbaseContext
    val broadCastHBaseConf = sc.broadcast(new SerializableWritable(hBaseConf) )
    val broadSaveJobConf = sc.broadcast(new SerializableWritable(SaveJobConf))


  //  val broadCastIndex = sc.broadcast(new SerializableWritable(indexs))
  val broadCastIndex = sc.broadcast(indexs)
    val sqlcontext = new SQLContext(sc)


    val trajPoints = ssc.socketTextStream("localhost",19999,StorageLevel.MEMORY_AND_DISK_SER)
    //每一妙的数据作为一个 batch,每个数据以时间为key,记录内容为value
    val record = trajPoints.map(TaxiMessage.parseMessage(_,bcIndexes.value)).filter(x=>x.region==Long.MaxValue)

    //Dstram 写 HBase文件
    record.foreachRDD(rdd=>{
      rdd.map(x=>TaxiMessage.convertToPut(x)).saveAsNewAPIHadoopDataset(broadSaveJobConf.value.value)
    })

    record.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val connection = ConnectionFactory.createConnection(broadCastHBaseConf.value.value)

        val table = connection.getTable(TableName.valueOf("test"))

        //if not over the threshold（read hbase!）
        // write to hbase
        //else{将这部分数据对应的空间里面的数据 写出并删掉，再写入}

      })
    })


 /*   record.foreachRDD( (message:RDD[(Int,Short,Float,Float,Int)], time: Time)=>{
      import sqlcontext.implicits._
      //使用DataFrame进行查询
      val messageDataFrame = message.map(x => TaxiMeesage(0,x._1,x._2,x._3,x._4,x._5)).toDF()

      messageDataFrame.registerTempTable("TaxiMessages")
      sqlcontext.cacheTable("TaxiMessages")
      val messageCountDF = sqlcontext.sql("select id, count(id) as total from TaxiMessages where id <100 group by id")
      messageCountDF.show()
      val speedDF = sqlcontext.sql("select * from TaxiMessages where speed<50")
      speedDF.show()
    })*/


    ssc.start()
    ssc.awaitTermination()
  }

  def createTable(hAdmin: HBaseAdmin,tableName:String):TableName={

    val messageTable = TableName.valueOf(tableName)
    if(hAdmin.tableExists(messageTable)){
      println("table: "+tableName+" exists! Delete it and recreate it now!")

    }else{
      // keep CF names as small as possible, meanwhile keep the columnQualifier small enough
      val tableDesc = new HTableDescriptor(messageTable)

      val cf2 = new HColumnDescriptor("as")
      tableDesc.addFamily(cf2)

      hAdmin.createTable(tableDesc)

      hAdmin.close()
      println("create Finished")
    }
    messageTable
  }

  case class TaxiMessage(region:Long,time:Long,id:Int,longitude:Float,latitude:Float,speed:Int)extends Serializable

  object TaxiMessage extends Serializable{
      def parseMessage(str: String,index :Array[CellInfo]):TaxiMessage={
        val temp= str.split("\t")
        if(temp.length!=2)
          throw new IllegalArgumentException("read data:"+temp+" error, because its elements is not two")
        val attrs = temp(1).split(",")
        val carID = attrs(0).toShort
        val longitude = attrs(1).toFloat
        val latitude = attrs(2).toFloat
        val region = SpatialUtilFuncs.getLocatedRegion(longitude,latitude,index)
        val speed = attrs(3).toFloat.toInt

        if(region.equals(-1)){
          TaxiMessage(Long.MaxValue, temp(0).toLong,carID, longitude, latitude, speed)
        }else{
          val hashCarID = carID%7

          val split = (region.toLong <<32) + hashCarID          //region:hashCarID

          TaxiMessage(split, temp(0).toLong,carID, longitude, latitude, speed)
          //split time           id      logi      lat      spped
        }


      }

    def convertToPut(ms: TaxiMessage):(ImmutableBytesWritable,Put)={
      val key = Bytes.toBytes(ms.region) ++ Bytes.toBytes(ms.id) ++ Bytes.toBytes(ms.time)
      val put = new Put(key)
  /*    final val family = Bytes.toBytes("as")
      final  val c_speed = Bytes.toBytes("s")
      final  val c_log = Bytes.toBytes("lo")
      final  val c_lat = Bytes.toBytes("la")*/
      put.add(family,c_log,Bytes.toBytes(ms.longitude))
      put.add(family,c_lat,Bytes.toBytes(ms.latitude))
      put.add(family,c_speed,Bytes.toBytes(ms.speed))
      return (new ImmutableBytesWritable(key),put)
    }
  }


}
