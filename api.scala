class api {

  /// simple API response
  import scalaj.http._

  /// Replace website and parameters to desired specifications
  val response: HttpResponse[String] = Http("http://foo.com/search").param("q","monkeys").asString
  response.body
  response.code
  response.headers
  response.cookies
}
