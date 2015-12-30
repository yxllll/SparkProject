package QueryPac

import scala.collection.mutable.ArrayBuffer


/**
 * Created by yang on 15-12-15.
 */
class queryByMBR {

  var minLng: Double = 0
  var minLat: Double = 0
  var maxLng: Double = 0
  var maxLat: Double = 0

  def this(minLng: Double, minLat: Double, maxLng: Double, maxLat: Double){
    this()
    this.minLng = minLng
    this.minLat = minLat
    this.maxLng = maxLng
    this.maxLat = maxLat
  }

  //判断是否在某一区域
  def isInMbr(trojdata: queryTrajDataFormat, mbr: queryByMBR) = {
    if(trojdata.lng > mbr.minLng && trojdata.lng < mbr.maxLng && trojdata.lat > mbr.minLat && trojdata.lat < mbr.maxLat)
      true
    else
      false
  }

  //查询该段数据中经过mbr区域的车辆,返回一个ArrayBuffer
  def getAllCarIDs(trajBuff: ArrayBuffer[queryTrajDataFormat], mbr: queryByMBR)={
    val passByCars = ArrayBuffer[Int]()
    for(i <- 0 until trajBuff.length){
      if(isInMbr(trajBuff(i), mbr)){
        passByCars += trajBuff(i).carID
      }
    }
    //去除重复元素
    passByCars.distinct
    passByCars
  }

  //查询某一ID在mbr内的平均速度
  def getSingleSpeedByCarID(carid: Int, trajBuff: ArrayBuffer[queryTrajDataFormat], mbr: queryByMBR) = {
    var avgspeed: Double = 0.0
    var countnum: Int = 0
    for(i <- 1 until trajBuff.length){
      if(trajBuff(i).carID.equals(carid) && isInMbr(trajBuff(i), mbr)){
        countnum += 1
        avgspeed = avgspeed + (trajBuff(i).speed - avgspeed)/countnum
      }
    }
    avgspeed
  }

  //查询所有车的平均速度,最高速度，最低速度
  def getAllCarsAverageSpeed(trajBuff: ArrayBuffer[queryTrajDataFormat], mbr: queryByMBR) = {
    val allcars = getAllCarIDs(trajBuff, mbr)
//    var averagrspeed: Double = getSingleSpeedByCarID(allcars(0), trajBuff, mbr)
    var averagrspeed: Double = 0
    for(i <- 1 until allcars.length){
      averagrspeed = averagrspeed + (getSingleSpeedByCarID(allcars(i), trajBuff, mbr) - averagrspeed)/i
    }
    averagrspeed
  }

  //最大速度的车ID和速度
  def getMaxSpeedAndCar(trajBuff: ArrayBuffer[queryTrajDataFormat], mbr: queryByMBR) = {
    val allcars = getAllCarIDs(trajBuff, mbr)
    var maxspeed: Double = 0.0
    var maxCarID: Int = 0
    for(i <- 0 until allcars.length){
      val tempspeed = getSingleSpeedByCarID(allcars(i), trajBuff, mbr)
      if(tempspeed > maxspeed){
        maxCarID = allcars(i)
        maxspeed = tempspeed
      }
    }
    (maxCarID,maxspeed)
  }

  //最小速度的车和ID
  def getMinSpeedAndCar(trajBuff: ArrayBuffer[queryTrajDataFormat], mbr: queryByMBR) = {
    val allcars = getAllCarIDs(trajBuff, mbr)
    var minCarID: Int = 0
    var minspeed: Double = 0.0
    for(i <- 0 until allcars.length){
      val tempspeed = getSingleSpeedByCarID(allcars(i), trajBuff, mbr)
      if(tempspeed < minspeed){
        minCarID = allcars(i)
        minspeed = tempspeed
      }
    }
    (minCarID, minspeed)
  }

  //查询速度高于某一值的一些车
  def getHighSpeedCars(speedLimit: Double, trajBuff: ArrayBuffer[queryTrajDataFormat], mbr: queryByMBR) = {
    val allcars = getAllCarIDs(trajBuff, mbr)
    val highspeeds = new ArrayBuffer[Double]()
    for(i <- 0 until allcars.length){
      val tempspeed = getSingleSpeedByCarID(allcars(i), trajBuff, mbr)
      if(tempspeed >= speedLimit){
        highspeeds.append(tempspeed)
      }

    }
    highspeeds
  }

/*  //查询瞬时最高速度
  def queryMaxSpeed(trajBuff: ArrayBuffer[queryTrajDataFormat], mbr: QueryByMBR) = {
    var  maxspeed = trajBuff(0).speed
    for(i <- 1 to trajBuff.length){
      if(isInMbr(trajBuff(i), mbr) && trajBuff(i).speed > maxspeed){
        maxspeed = trajBuff(i).speed
      }
    }
    maxspeed
  }

  //查询瞬时最低速度
  def queryMinSpeed(trajBuff: ArrayBuffer[queryTrajDataFormat], mbr: QueryByMBR) = {
    var  minspeed = trajBuff(0).speed
    for(i <- 1 to trajBuff.length){
      if(isInMbr(trajBuff(i), mbr) && trajBuff(i).speed < minspeed){
        minspeed = trajBuff(i).speed
      }
    }
    minspeed
  }*/

}

