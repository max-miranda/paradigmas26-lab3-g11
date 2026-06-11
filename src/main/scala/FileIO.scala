import scala.io.Source
import scala.util.Using
import org.json4s._
import org.json4s.jackson.JsonMethods._

object FileIO {

  /**
   * Read subscriptions from JSON file.
   * @param filePath path to subscriptions file
   * @return list of options: Some(Subscription) for valid entries, None for malformed entries
   *         returns empty list if file not found
   */
  def readSubscriptions(filePath: String): List[Option[Subscription]] = {
    try {
      implicit val formats: Formats = DefaultFormats
      val content = Using.resource(Source.fromFile(filePath)) { source =>
        source.mkString
      }

      parse(content).children.map { sub =>
        val nameOpt = (sub \ "name").extractOpt[String]
        val urlOpt = (sub \ "url").extractOpt[String]

        (nameOpt, urlOpt) match {
          case (Some(name), Some(url)) => Some(Subscription(name, url))
          case _ =>
            println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
            None
        }
      }
    } catch {
      case _: java.io.FileNotFoundException =>
        println(s"Error: Could not load $filePath - file not found")
        List()

      case _: org.json4s.ParserUtil.ParseException =>
        println(s"Error: Could not load $filePath - invalid JSON format")
        List()

      case _: Exception =>
        println(s"Error: Could not load $filePath - invalid JSON format")
        List()
    }
  }

  /**
   * Download feed JSON from URL.
   * @param url Reddit feed URL
   * @return Option containing JSON as String, None on network error or timeout
   */
  def downloadFeed(url: String): Option[String] = {
    try {
      Some(Using.resource(Source.fromURL(url)) { source =>
        source.mkString
      })
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Read dictionary file line by line.
   * @param filePath path to dictionary file
   * @return Option containing list of entities, None if file missing or unreadable
   */
  def readDictionaryFile(filePath: String): Option[List[String]] = {
    try {
      Some(Using.resource(Source.fromFile(filePath)) { source =>
        source.getLines()
          .map(_.trim)
          .filter(_.nonEmpty)
          .filterNot(_.startsWith("#"))
          .toList
      })
    } catch {
      case _: Exception => None
    }
  }
}
