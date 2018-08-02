import dispatch.Http
import dispatch.Defaults._
import dispatch._
import play.api.libs.json._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

//HttpEntity.Strict(application/json,{
// "place_id":"1201728",
// "licence":"Data © OpenStreetMap contributors, ODbL 1.0. http:\/\/www.openstreetmap.org\/copyright",
// "osm_type":"way",
// "osm_id":"265417761",
// "lat":"40.74727145",
// "lon":"-73.9773630832979",
// "display_name":"544, 3rd Avenue, Tudor City, Manhattan Community Board 6, New York County, NYC, 10016, United States of America",
// "address":{"house_number":"544","road":"3rd Avenue","neighbourhood":"Tudor City","city":"NYC","county":"New York County","postcode":"10016","country":"United States of America","country_code":"us"}})

final case class NominatimJson(place_id: Int, licence: String, osm_type: String, osm_id: String,
                               lat: Double, lon: Double, display_name: String, address: Map[String,String])

object NominatimClient {
  def makeReqURI(host: String, hostPort: String, lat: Double, lon: Double) =
    s"http://$host:$hostPort/reverse.php?format=json&lat=$lat&lon=$lon"

  def main(args: Array[String]): Unit = {

    val host = "localhost"
    val hostPort = "8080"
    val lat = 40.747166999999997
    val lon = -73.977132999999995
    val reqURI = makeReqURI(host, hostPort, lat, lon)

    val req = dispatch.url(reqURI)
    val jsonFuture: Future[JsValue] = Http.default(req OK as.String) map { s => Json.parse(s)}
    val json = Await.result(jsonFuture, 60.seconds)
    println((json \ "address" \ "postcode").get)


  }

}
