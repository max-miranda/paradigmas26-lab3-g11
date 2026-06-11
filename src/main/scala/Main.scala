import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    //----------------------------------------
    // EJERCICIO 1

    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    val spark = SparkSession.builder()                                                  // si se parsean bien los comandos, creo la sesion de spark
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten

    if (subscriptions.isEmpty) {                                                        // si no tengo ninguna subscripcion, termino la ejecucion
      println("Error: No valid subscriptions found")
      spark.stop()
      sys.exit(1)
    }

    val entitiesDir = new java.io.File(cmdArgs.entitiesDir)
    if (!entitiesDir.exists() || !entitiesDir.isDirectory) {
      println(s"Error: entities directory '${cmdArgs.entitiesDir}' not found")
      spark.stop()
      sys.exit(1)
    }

    // Load dictionaries on the driver before distributing the entity detection work.
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    val subscriptionsRDD: RDD[Subscription] = sc.parallelize(subscriptions)             // cargo las subscriptions en RDD

    val feedsSuccessAcc = sc.longAccumulator("feeds exitosos")                          // contadores de feeds
    val feedsFailedAcc = sc.longAccumulator("feeds fallidos")                           // son como variables globales para que todos los workers puedan contar

    val postsSuccessAcc = sc.longAccumulator("posts exitosos")                          // contador de posts
    val postsFilteredAcc = sc.longAccumulator("posts filtrados")
    val postsFailedAcc = sc.longAccumulator("posts fallidos")
    val postsTotalAcc = sc.longAccumulator("posts totales")

    val totalCharsAcc = sc.longAccumulator("total de caracteres de posts filtrados")

    val preDownload = System.currentTimeMillis()

    // Download feeds and parse posts, tracking success/failure
    val downloadResultsRDD: RDD[Post] = subscriptionsRDD.flatMap { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)                                           // descargo el feed

      if (feedOpt.isDefined) {                                                                      // si se pudo descargar el feed,
        val parsed = JsonParser.parsePostsWithFailures(feedOpt.get, subscription.name, subscription.url)
        val posts = parsed.posts
        feedsSuccessAcc.add(1)
        postsSuccessAcc.add(posts.length)
        postsFailedAcc.add(parsed.failedPosts)
        postsTotalAcc.add(posts.length + parsed.failedPosts)

        val postValidos = Analyzer.filterEmptyPosts(posts)                                          // filtro los que tengan titulo o texto vacio
        val filtrados = posts.length - postValidos.length
        postsFilteredAcc.add(filtrados)

        postValidos.foreach { post =>
          totalCharsAcc.add(post.title.length + post.selftext.length)
        }

        postValidos.toIterator

      } else {                                                                                      // fallo la descarga del feed
        feedsFailedAcc.add(1)
        println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
        Iterator.empty
      }
    }.cache()

    // spark es lazy, si no lo obligo no va a procesar los datos
    val validPostsCount = downloadResultsRDD.count()

    val postDownload = System.currentTimeMillis()

    // Count feed successes/failures
    val feedsSuccess = feedsSuccessAcc.value.toInt
    val feedsFailed = feedsFailedAcc.value.toInt

    // Flatten all posts and count JSON parse failures
    val postsSuccess = postsSuccessAcc.value.toInt            // todos los posts que se descargaron correctamente
    val postsFiltered = postsFilteredAcc.value.toInt          // posts que fueron filtrados/descartados por tener titulo o texto vacio
    val postsValid = validPostsCount.toInt                    // posts validos (con titulo y texto)
    val postsFailed = postsFailedAcc.value.toInt
    val postsTotal = postsTotalAcc.value.toInt

    // Calculate average characters in filtered posts
    val totalChars = totalCharsAcc.value.toInt
    val avgChars = if (postsValid != 0) totalChars / postsValid else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess,
      "feedsFailed" -> feedsFailed,
      "postsSuccess" -> postsSuccess,
      "postsFailed" -> postsFailed,
      "postsFiltered" -> postsFiltered,
      "postsTotal" -> postsTotal,
      "avgChars" -> avgChars
    )

    if (postsValid == 0) {
      println("Error: No valid posts downloaded after filtering")
      downloadResultsRDD.unpersist()
      spark.stop()
      sys.exit(1)
    }

    // El diccionario sera compartido entre los workers, asi no copian el mismo diccionario una y otra vez
    val brodDictionary = sc.broadcast(dictionary)

    val preEntities = System.currentTimeMillis()

    // Detect entities in all posts (combine title and selftext)
    val allEntities: RDD[NamedEntity] = downloadResultsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, brodDictionary.value)
    }

    val pairs = allEntities
      .map { e => ((e.entityType, e.text), 1) }
      .reduceByKey(_ + _)
      .sortBy { case ((entityType, entityName), count) => (-count, entityType, entityName) }

    val unformatted = pairs.take(cmdArgs.topK)

    downloadResultsRDD.unpersist()

    val postEntities = System.currentTimeMillis()

    val downloadTime = (postDownload - preDownload) / 1000f
    val entitiesTime = (postEntities - preEntities) / 1000f
    val totalTimePipeline = downloadTime + entitiesTime

    println(Formatters.formatProcessingStats(stats))
    println(Formatters.formatedEntities(unformatted))
    println(s"== Tiempo descarga y filtrado: $downloadTime ==")
    println(s"== Tiempo entidades y reduccion: $entitiesTime ==")
    println(s"== Tiempo total de Pipelining: $totalTimePipeline ==")
  }
}
