package Tests

/**
 * Created by yang on 15-12-29.
 */
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yang on 15-12-3.
 */
object wordCount {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }
    val conf = new SparkConf().setMaster("localhost").setAppName("WordCount")
    val sc = new SparkContext(conf)
    //    val line = sc.textFile(args(0))
    val line = sc.textFile("/home/yang/Desktop/Stream")
    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)

    sc.stop()
  }
}

