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

    if (subscriptions.length == 0) {                                                    // si no tengo ninguna subscripcion, termino la ejecucion
      println("Error: No valid subscriptions found")
      sys.exit(1)
    }

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
      val feedOpt = FileIO.downloadFeed(subscription.url)                                           // descargo el

      if (feedOpt.isDefined) {                                                                      // si se pudo descargar el feed, 
        val posts = JsonParser.parsePosts(feedOpt.get, subscription.name, subscription.url)         // parseo los posts
        feedsSuccessAcc.add(1)
        postsSuccessAcc.add(posts.length)
        postsTotalAcc.add(posts.length)
        val postValidos = Analyzer.filterEmptyPosts(posts)                                          // filtro los que tengan titulo o texto vacio
        
        val filtrados = posts.length - postValidos.length
        postsFilteredAcc.add(filtrados)

        if (postValidos.isEmpty) {
          println("Error: No valid posts downloaded after filtering") //Che fijarse si este print hara un error a futuro, porque los workers no deberian usar prints
          postsFailedAcc.add(1)
          postsTotalAcc.add(1)
        }

        postValidos.foreach { post =>
          totalCharsAcc.add(post.title.length + post.selftext.length)
        }

        postValidos.toIterator   

      } else {                                                                                      // fallo la descarga del feed
        feedsFailedAcc.add(1)
        println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
        List()    
      } 
    }.cache()

    // spark es lazy, si no lo obligo no va a procesar los datos
    downloadResultsRDD.count()

    val postDownload = System.currentTimeMillis()

    // Count feed successes/failures
    val feedsSuccess = feedsSuccessAcc.value.toInt
    val feedsFailed = feedsFailedAcc.value.toInt

    // Flatten all posts and count JSON parse failures
    val postsSuccess = postsSuccessAcc.value.toInt            // todos los posts que se descargaron correctamente
    val postsFiltered = postsFilteredAcc.value.toInt          // posts que fueron filtrados/descartados por tener titulo o texto vacio
    val postsValid = postsSuccess - postsFiltered             // posts validos (con titulo y texto)
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

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir) // Diccionario cargado SOLO en driver

    // El diccionario sera compartido entre los workers, asi no copian el mismo diccionario una y otra vez
    val brodDictionary = sc.broadcast(dictionary)

    val preEntities = System.currentTimeMillis()

    // Detect entities in all posts (combine title and selftext)
    val allEntities : RDD[NamedEntity] = downloadResultsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, brodDictionary.value)
    }

    val pairs = allEntities
      .map{ e => ((e.entityType, e.text), 1) }
      .reduceByKey(_ + _)
      .sortBy{ e => (e._1._1, -e._2) }

    val unformatted = pairs.collect()

    downloadResultsRDD.unpersist()

    val postEntities = System.currentTimeMillis()

    val totalTimePipeline = ((postEntities - preEntities) + (postDownload - preDownload)) / 1000f

    println(Formatters.formatProcessingStats(stats))
    println(Formatters.formatedEntities(unformatted))
    println(s"== Tiempo total de Pipelining: ${totalTimePipeline} ==")

    // paro spark para cerrar la sesión Spark, liberar memoria, cerrar conexiones, etc
    spark.stop()
  }
}