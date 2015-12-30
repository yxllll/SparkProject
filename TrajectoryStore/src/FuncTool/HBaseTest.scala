package FuncTool



import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import org.apache.hadoop.fs.Path
/**
  * Created by zzg on 15-12-25.
  */
object HBaseTest {
  def main(args: Array[String]): Unit = {
 //   val conf = new SparkConf().setMaster("local[4]").setAppName("DataReceiver")
   // org.apache.hadoop.hbase.client.RpcRetryingCallerFactory
    val hBaseConf  = HBaseConfiguration.create()
    //集群环境下 要使每个worker node访问不了hbase内容。原因我们之前的例子里没有指定hbase的地址信息
    hBaseConf.set("hbase.zookeeper.quorum","localhost")//指定ip地址
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181") // zookeeper的端口号
    hBaseConf.addResource(new Path("/home/zzg/Softwares/hbase/conf/hbase-site.xml"))
 //   val sc = new SparkContext(conf)

//    val broadCastHBaseConf = sc.broadcast(new SerializableWritable(hBaseConf) )
   /* val a =sc.parallelize(1 to 9,3)

    a.foreach(x=>println(x))
    a.foreach(x=>println(x))*/

    val methods = new org.apache.hadoop.hbase.util.Addressing().getClass.getMethods
    methods.foreach(method=>println(method.getName))

   /* def urlses(cl: ClassLoader): Array[java.net.URL] = cl match {
      case null => Array()
      case u: java.net.URLClassLoader => u.getURLs() ++ urlses(cl.getParent)
      case _ => urlses(cl.getParent)
    }
    val urls = urlses(getClass.getClassLoader)
    urls.foreach(url=>println(url.toString))*/

    try{
      val connection = ConnectionFactory.createConnection(hBaseConf)

      val table = connection.getTable(TableName.valueOf("test"))

      val scan = new Scan()
      val resultScanner = table.getScanner(scan)
      var res = resultScanner.next()
      while(res!=null){
        val rowBytes = res.getRow
        val key = Bytes.toString(rowBytes)
        println("ok")
        println(key)
        res = resultScanner.next()
      }
    }catch {
      case a: Exception=> a.printStackTrace()
    }

  }
}
