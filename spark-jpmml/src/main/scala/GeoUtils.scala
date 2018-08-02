package xyz.akumano.scala.beam.util

// ref: https://github.com/dataArtisans/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/utils/GeoUtils.java
object GeoUtils {

  type Lat = Double
  type Lon = Double
  type GridCellId = Int
  case class Coord(lat: Lat, lon: Lon)

  val LAT_NORTH = 41.0
  val LAT_SOUTH = 40.5
  val LAT_HEIGHT = LAT_NORTH - LAT_SOUTH
  // the number of grids in the N-S direction
  val NGRID_Y: Int = 400
  val DELTA_Y: Double = LAT_HEIGHT/NGRID_Y // => 0.00125

  val LON_EAST = -73.70
  val LON_WEST = -74.05
  val LON_WIDTH = LON_EAST - LON_WEST
  // the number of grids in the W-E direction
  val NGRID_X: Int = 250
  val DELTA_X: Double = LON_WIDTH/NGRID_X // => 0.0014

  private def latInNYC(lat: Lat) =
    LAT_SOUTH < lat & lat < LAT_NORTH
  private def lonInNYC(lon: Lon) =
    LON_WEST < lon & lon < LON_EAST

  def isInNYC(z: Coord) = {
    z match {
      case Coord(lat, lon) => latInNYC(lat) & lonInNYC(lon)
    }
  }

  @throws(classOf[IllegalArgumentException])
  def toGridCell(z: Coord): GridCellId = {
    if (!isInNYC(z)) throw new IllegalArgumentException("Illegal lat-lon coordinates: ("
      + z.lat + "," + z.lon + ").")
    z match {
      case Coord(lat, lon) =>
        val xIdx = (Math.floor( (lon - LON_WEST) / DELTA_X )).toInt
        val yIdx = (Math.floor( (LAT_NORTH - lat) / DELTA_Y )).toInt
        NGRID_X * yIdx + xIdx
    }
  }

  private def gridCellCenterLat(id: GridCellId): Lat = {
    val xIdx = id & NGRID_X
    val yIdx = (id - xIdx) / NGRID_X
    (LAT_NORTH - (yIdx * DELTA_Y) - (DELTA_Y/2))
  }
  private def gridCellCenterLon(id: GridCellId): Lon = {
    val xIdx = id & NGRID_X
    ((Math.abs(LON_WEST) - (xIdx * DELTA_X) - DELTA_X/2) * -1.0f)
  }
  def getIndex(id: GridCellId): (Int, Int) = {
    (id % NGRID_X, id / NGRID_X)
  }

  // TODO: test this
  def gridCellCenter(id: GridCellId): Coord = new Coord(gridCellCenterLat(id), gridCellCenterLon(id))

  def main(args: Array[String]) = {
    println(toGridCell(new Coord((LAT_NORTH - 0.00001), (LON_WEST + 0.00001))))
    println(toGridCell(new Coord((LAT_NORTH - 0.00001), (LON_EAST - 0.00001))))
    println(toGridCell(new Coord((LAT_SOUTH + 0.00001), (LON_EAST - 0.00001))))
  }
}
