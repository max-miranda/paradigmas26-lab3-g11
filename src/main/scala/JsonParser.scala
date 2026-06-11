import org.json4s._
import org.json4s.jackson.JsonMethods._

case class PostParseResult(posts: List[Post], failedPosts: Int)

object JsonParser {

  /**
   * Parse Reddit JSON feed and extract posts.
   * @param jsonContent JSON string from Reddit API
   * @param subscriptionName name of subscription (for logging)
   * @param subscriptionUrl url of subscription
   * @return posts successfully parsed and number of failed post parses
   */
  def parsePostsWithFailures(jsonContent: String, subscriptionName: String, subscriptionUrl: String): PostParseResult = {
    try {
      implicit val formats: Formats = DefaultFormats

      val json = parse(jsonContent)
      val children = (json \ "data" \ "children").extract[List[JValue]]

      val results = children.map { child =>
        try {
          val data = child \ "data"
          val title = (data \ "title").extract[String]
          val selftext = (data \ "selftext").extract[String]
          Right(Post(title, selftext))
        } catch {
          case _: Exception =>
            println(s"Warning: Failed to parse posts from '$subscriptionName' ($subscriptionUrl)")
            Left(())
        }
      }

      PostParseResult(
        results.collect { case Right(post) => post },
        results.count(_.isLeft)
      )
    } catch {
      case _: Exception =>
        println(s"Warning: Failed to parse posts from '$subscriptionName' ($subscriptionUrl)")
        PostParseResult(List(), 1)
    }
  }

  /**
   * Backwards-compatible helper for callers that only need parsed posts.
   */
  def parsePosts(jsonContent: String, subscriptionName: String, subscriptionUrl: String): List[Post] = {
    parsePostsWithFailures(jsonContent, subscriptionName, subscriptionUrl).posts
  }
}
