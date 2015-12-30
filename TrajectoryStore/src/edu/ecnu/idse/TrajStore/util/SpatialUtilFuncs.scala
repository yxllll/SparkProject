package edu.ecnu.idse.TrajStore.util

import edu.ecnu.idse.TrajStore.core.CellInfo

import scala.util.control.Breaks._

/**
  * Created by zzg on 15-12-23.
  */
object SpatialUtilFuncs {

  def getLocatedRegion(lg:Float, la:Float, regions :Array[CellInfo]): Int ={
    var result  = -1
    breakable {
      for (i <- 0 until regions.length) {
        if (regions(i).getMBR.contains(lg, la)) {
          result = regions(i).cellId
          break
        }
      }
    }
    result
  }
}
