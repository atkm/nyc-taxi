package xyz.akumano.beam.popularareas;

// ref: https://github.com/dataArtisans/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/utils/GeoUtils.java
public class GeoUtils {

  public static class Coord {
    private final double lat, lon;

    public Coord(double lat, double lon) {
      this.lat = lat;
      this.lon = lon;
    }

    public double getLat() {
      return lat;
    }
    public double getLon() {
      return lon;
    }
  }

  public static double LAT_NORTH = 41.0;
  public static double LAT_SOUTH = 40.5;
  public static double LAT_HEIGHT = LAT_NORTH - LAT_SOUTH;
  // the number of grids in the N-S direction
  public static int NGRID_Y = 400;
  public static double DELTA_Y = LAT_HEIGHT/NGRID_Y; // => 0.00125

  public static double LON_EAST = -73.70;
  public static double LON_WEST = -74.05;
  public static double LON_WIDTH = LON_EAST - LON_WEST;
  // the number of grids in the W-E direction
  public static int NGRID_X = 250;
  public static double DELTA_X = LON_WIDTH/NGRID_X; // => 0.0014

  private static boolean latInNYC(double lat) {
    return LAT_SOUTH < lat && lat < LAT_NORTH;
  }
  private static boolean lonInNYC(double lon) {
    return LON_WEST < lon && lon < LON_EAST;
  }

  public static boolean isInNYC(double lat, double lon) {
    return latInNYC(lat) && lonInNYC(lon);
  }

  public static int toGridCell(Coord z) throws IllegalArgumentException {
    double lat = z.lat, lon = z.lon;
    if (!isInNYC(lat, lon)) {
      throw new IllegalArgumentException("Illegal lat-lon coordinates: ("
          + lat + "," + lon + ").");
    }
    // casting to int rounds down. The same as floor.
    int xIdx = (int) ((lon - LON_WEST) / DELTA_X );
    int yIdx = (int) ( (LAT_NORTH - lat) / DELTA_Y );
    return NGRID_X * yIdx + xIdx;
  }

  private static double gridCellCenterLat(int id) {
    int xIdx = id % NGRID_X;
    int yIdx = (id - xIdx) / NGRID_X;
    return LAT_NORTH - (yIdx * DELTA_Y) - (DELTA_Y/2);
  }
  private static double gridCellCenterLon(int id) {
    int xIdx = id % NGRID_X;
    return (Math.abs(LON_WEST) - (xIdx * DELTA_X) - DELTA_X/2) * -1.0f;
  }
  //def getIndex(id: GridCellId): (Int, Int) = {
  //  (id % NGRID_X, id / NGRID_X)
  //}

  public static Coord gridCellCenter(int id) {
    return new Coord(gridCellCenterLat(id), gridCellCenterLon(id));
  }

  //public static void main(String[] args) {
  //  println(toGridCell(new Coord((LAT_NORTH - 0.00001), (LON_WEST + 0.00001))));
  //  println(toGridCell(new Coord((LAT_NORTH - 0.00001), (LON_EAST - 0.00001))));
  //  println(toGridCell(new Coord((LAT_SOUTH + 0.00001), (LON_EAST - 0.00001))));
  //}
}
