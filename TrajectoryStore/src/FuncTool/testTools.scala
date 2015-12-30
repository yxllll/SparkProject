package FuncTool

import edu.ecnu.idse.TrajStore.util.TypeConvert
import org.apache.hadoop.hbase.util.Bytes

import scala.util.control.Breaks._

/**
  * Created by zzg on 15-12-23.
  */
object testTools {
  def main(args: Array[String]): Unit = {
    val rec =  Array(1,19,7,11,5)
    val c =  breakTest(rec)
    println(c)

/*    val a =0xff00
    val b = 0x0001*/
    val a =0x0000ff00
    val b = 0x00000001
    val k = b<<2
    println(k)
    println("a:")
    Bytes.toBytes(a).foreach(x=>print(x)) //00-10
    println()
    println("b:")
    Bytes.toBytes(b).foreach(x=>print(x))//0001
    println()
  println("a++b:")
    val cd = Bytes.toBytes(a)++ Bytes.toBytes(b)

    cd.foreach(x=>print(x)) //00-100001
    println()
    val ef = (a.toLong<<32) + b
    println(ef)
    var loop = ef
    var i=0
    while(loop>0){
      val tmp = loop & 0xff
      println("i=" +i)
      print("& value="+tmp+"\t")
      val bytes = TypeConvert.shortToBytes(tmp.toShort)
      var bye = 0
      while(bye<bytes.length){
      print(bytes(bye))
        bye = bye +1
      }
      i=i+1
      loop = loop >>>8
      println(" left: "+ loop)
    }
  }

  def breakTest(array:Array[Int]):Int={
    var result = -1
    var count = 0
    breakable{
      for(i<-0 until array.length){
        count+= 1
        if(array(i) > 12){
          result = array(i)
          break
        }
      }
    }
  println(count)
    result
  }
}
