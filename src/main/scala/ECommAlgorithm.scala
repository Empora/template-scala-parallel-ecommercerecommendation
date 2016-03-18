package org.template.ecommercerecommendation

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params
import io.prediction.data.storage.BiMap
import io.prediction.data.storage.Event
import io.prediction.data.store.LEventStore
import io.prediction.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{ Rating => MLlibRating }
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors

import scala.util.Sorting
import scala.collection.mutable.PriorityQueue
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

import org.joda.time.DateTime
import breeze.linalg.shuffle
import grizzled.slf4j.Logger

import java.util.Calendar
import java.text.SimpleDateFormat
import java.io._

/**
 * A list of parameters for the e-commerce algorithm, these parameters are specified
 * in the engine.json file of the app
 */
case class ECommAlgorithmParams(
  appName: String,
  unseenOnly: Boolean,
  seenEvents: List[String],
  similarEvents: List[String],
  rank: Int,
  numIterations: Int,
  lambda: Double,
  seed: Option[Long]) extends Params

/**
 * an instance of this class is stored in the final model for each item that is used
 * in the learning process
 */
case class ProductModel(
  item: Item, // the id of the item
  features: Option[Array[Double]], // features by ALS
  count: Int // popular count for default score
  )

/**
 * rank: Int dimension of feature vectors,
 * userFeatures: Map[Int, Array[Double]] feature vectors,
 * productModels: Map[Int, ProductModel] id to product model map
 * userObjects: Option[Map[Int,User]])
 */
class ECommModel(
    val rank: Int, // dimension of feature vectors
    val userFeatures: Map[Int, Array[Double]],
    val productModels: Map[Int, ProductModel],
    val userObjects: Option[Map[Int, User]]) extends Serializable {

  //  @transient lazy val itemIntStringMap = itemStringIntMap.inverse

  override def toString = {
    s" rank: ${rank}" +
      s" userFeatures: [${userFeatures.size}]" +
      s"(${userFeatures.take(2).toList}...)" +
      s" productModels: [${productModels.size}]" +
      s"(${productModels.take(2).toList}...)"
  }
}

class ECommAlgorithm(val ap: ECommAlgorithmParams)
    extends P2LAlgorithm[PreparedData, ECommModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): ECommModel = {

    //=============================================================================
    // FORMER CHECK WHETHER DATA IS PRESENT, OMITTED FOR PERFORMANCE REASONS
    // (THE TAKE METHOD IS EXPENSIVE ON LARGE DATA SET)
    //    require(!data.likeEvents.take(1).isEmpty,
    //      s"likeEvents in PreparedData cannot be empty." +
    //        " Please check if DataSource generates TrainingData" +
    //        " and Preprator generates PreparedData correctly.")
    //    require(!data.items.take(1).isEmpty,
    //      s"items in PreparedData cannot be empty." +
    //        " Please check if DataSource generates TrainingData" +
    //        " and Preprator generates PreparedData correctly.")
    //=============================================================================

    val mllibRatings: RDD[MLlibRating] = genMLlibRating(data)

    //=============================================================================
    // FORMER CHECK WHETHER THE RATINGS ARE NON-EMPTY. THIS ACTUALLY IS THE
    // BASIC INFORMATION FOR COMPUTING THE FEATURE VECTORS VIA ALS ALGORITHM
    // MLLib ALS cannot handle empty training data.
    //    require(!mllibRatings.take(1).isEmpty,
    //      s"mllibRatings cannot be empty." +
    //        " Please check if your events contain valid user and item ID.")
    //=============================================================================

    // seed for MLlib ALS
    val seed = ap.seed.getOrElse(System.nanoTime)

    // use ALS to train feature vectors
    val m = ALS.trainImplicit(
      ratings = mllibRatings,
      rank = ap.rank,
      iterations = ap.numIterations,
      lambda = ap.lambda,
      blocks = 100, // -1
      alpha = 100.0,
      seed = seed)

    val userFeatures = m.userFeatures.collectAsMap.toMap

    val items = data.items

    // join item with the trained productFeatures
    val productFeatures: Map[Int, (Item, Option[Array[Double]])] =
      items.leftOuterJoin(m.productFeatures).collectAsMap.toMap

    //    val popularCount = trainDefaultViews(data)
    val popularCount = trainDefaultLikes(data)
    //    val popularCount = trainDefault(data)

    val productModels: Map[Int, ProductModel] = productFeatures
      .map {
        case (index, (item, features)) =>
          val pm = ProductModel(
            item = item,
            features = features,
            // NOTE: use getOrElse because popularCount may not contain all items.
            count = popularCount.getOrElse(index, 0))
          (index, pm)
      }

    // get the map Int -> User for model
    val userObjects = data.users.collect().toMap

    logger.info("nr of product features: " + productModels.size)
    new ECommModel(
      rank = m.rank,
      userFeatures = userFeatures,
      productModels = productModels,
      userObjects = Some(userObjects))
  }

  /**
   * Generate MLlibRating from PreparedData.
   * You may customize this function if use different events or different aggregation method
   */
  def genMLlibRating(data: PreparedData): RDD[MLlibRating] = {

    logger.info(s"Getting mllib ratings from likeEvents: number of partitions: ${data.likeEvents.partitions.size}")

    val mllibRatings = data.likeEvents
      .map { r =>
        // get user and item Int IDs for MLlib
        val uindex = r.user
        val iindex = r.item

        ((uindex, iindex), 1)
      }
      .reduceByKey(_ + _) // aggregate all like events of same user-item pair
      .map {
        case ((u, i), v) =>
          // MLlibRating requires integer index for user and item
          // in the following line set v=1 to reduce the like events to one per user-item pair
          MLlibRating(u, i, v)
      }
    //.cache()
    //.persist(StorageLevel.MEMORY_AND_DISK_SER)

    mllibRatings
  }

  /**
   * Train default model.
   * You may customize this function if use different events or
   * need different ways to count "popular" score or return default score for item.
   */
  def trainDefault(
    data: PreparedData): Map[Int, Int] = {
    // count number of buys
    // (item index, count)
    val buyCountsRDD: RDD[(Int, Int)] = data.buyEvents
      .map { r =>
        // Convert user and item String IDs to Int index
        val uindex = r.user
        val iindex = r.item

        (uindex, iindex, 1)
      }
      .map { case (u, i, v) => (i, 1) } // key is item
      .reduceByKey { case (a, b) => a + b } // count number of items occurrence

    buyCountsRDD.collectAsMap.toMap
  }

  /**
   * Call this method, if the default return ( in case the user is unknown at all,
   * i.e. does not have a feature vector or any recently viewed/liked items )
   * should consist of the most viewed items
   */
  def trainDefaultViews(
    data: PreparedData): Map[Int, Int] = {
    val viewCountsRDD: RDD[(Int, Int)] = data.viewEvents
      .map { r =>
        // Convert user and item String IDs to Int index
        val uindex = r.user
        val iindex = r.item
        (uindex, iindex, 1)
      }
      .map { case (u, i, v) => (i, 1) } // key is item
      .reduceByKey { case (a, b) => a + b } // count number of items occurrence

    viewCountsRDD.collectAsMap.toMap
  }

  /**
   * Call this method, if the default return ( in case the user is unknown at all,
   * i.e. does not have a feature vector or any recently viewed/liked items )
   * should consist of the most liked items
   */
  def trainDefaultLikes(
    data: PreparedData): Map[Int, Int] = {
    val likeCountsRDD: RDD[(Int, Int)] = data.likeEvents
      .map { r =>
        // Convert user and item String IDs to Int index
        val uindex = r.user
        val iindex = r.item
        (uindex, iindex, 1)
      }
      .map { case (u, i, v) => (i, 1) } // key is item
      .reduceByKey { case (a, b) => a + b } // count number of items occurrence

    likeCountsRDD.collectAsMap.toMap
  }

  /**
   * the main prediction method, entry point for incoming queries from engine
   * ! do not rename -- predictionIO framework specific name !
   * here it is just decided whether to return standard recommendations for a specific user
   * or return item suggestions based on clustering result
   */
  def predict(model: ECommModel, query: Query): PredictedResult = {

    logger.info("************** NEW QUERY **************")
    val today = Calendar.getInstance().getTime()
    val beginTime = System.currentTimeMillis()
    val dateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val timestring = dateformat.format(today)
    logger.info("request arrived at " + timestring)
    logger.info("QUERY: " + query.toString())

    val method: String = query.method

    var predictedResult = new PredictedResult(null)
    method match {
      // if the method in query equals "recommend" then go into the template's standard
      // recommendation procedure
      case "recommend" => predictedResult = getRecommendations(model, query)
      // if the method in query equals "cluster" then go into clustering methods and return
      // outfit suggestions based on clustering result
      case "cluster" => {
        //        val conf = new SparkConf().setAppName("Clustering").setMaster("local")
                val sc_cluster: SparkContext = new SparkContext()
//                predictedResult = getOnboardingSuggestions(model, query, sc_cluster)

                predictedResult = getOnboardingSuggestions(model, query)
                sc_cluster.stop()

        // alternate onboarding suggestions without clustering (i.e. without
        // the need to instantiate a second SparkContext which is a critical action)
        predictedResult = getAlternateOnboardingSuggestions(model, query)
      }
    }

    val endTime = System.currentTimeMillis()
    val predictDuration = endTime - beginTime

    logger.info("prediction done -- duration: " + predictDuration.toString() + "ms")
    predictedResult
  }

  /**
   * compute item suggestions for the on-boarding approach for unknown user based on clustering
   * results
   */
  def getOnboardingSuggestions(model: ECommModel, query: Query): PredictedResult = {

    // first, get the input data for the clustering algorithm, i.e. the top liked items
    // of the specified time span which are contained in the model
    val nrItems: Int = 2000
    val daysBack: Long = 60L
    val maxIterations = 40
    val minCount = 10
    val numClusters = query.numberOfClusters.getOrElse(10)

    logger.info("computing " + nrItems.toString() + " most liked items of last " +
      daysBack.toString() + " days for cluster input")

    // get the top items for clustering input
    val topsForClustering: Array[(Int, ProductModel)] = computeTopsForClustering(model, daysBack, nrItems, minCount)
    // calculate the map itemID -> #likes
    val itemToCountArray: Array[(Int, Int)] = topsForClustering.map { case (id, pm) => (id, pm.count) }

    var itemClusters: Array[ItemScore] = Array()

    if (topsForClustering.length > 0) {

      val itemToClusterArray: Array[(Int, Int)] = runClusterAlgorithm(topsForClustering, maxIterations, numClusters)

      val returnCluster = query.returnCluster.getOrElse(-1)
      val itemToClusterMap = itemToClusterArray.toMap

      if (returnCluster < 0) {
        logger.info("no cluster specified, calculating over all clusters")
        itemClusters = collectOverAllClusters(itemToClusterArray, itemToCountArray, query)

      } else {
        logger.info("returning only from cluster " + returnCluster.toString() + ", num to return: " + query.num.toString())
        itemClusters = collectFromSpecificCluster(itemToClusterArray, itemToCountArray, query)
      }

    }
    new PredictedResult(itemClusters.toArray)
  }

  def getAlternateOnboardingSuggestions(model: ECommModel, query: Query): PredictedResult = {

    logger.info("get alternate onboarding suggestions")
    val daysBack: Long = 60L
    val nrItems: Int = query.num
    val minCount: Int = 10

    val topModels: Array[(Int, ProductModel)] = computeTopsForClustering(model, daysBack, nrItems, minCount)
    val itemToCountArray: Array[(Int, Int)] = topModels.map { case (id, pm) => (id, pm.count) }
    val itemToCountMap = itemToCountArray.toMap
    var idArray: Array[Int] = itemToCountArray.map(x => x._1)

    idArray = shuffle(idArray)

    logger.info("computed " + topModels.length + " top models")

    val itemScoreArray: Array[ItemScore] = idArray.map {
      id =>
        new ItemScore(
          item = id,
          score = itemToCountMap.get(id).get.toDouble,
          cluster = None)
    }

    logger.info("returning " + itemScoreArray.length + " outfits")
    new PredictedResult(itemScoreArray)
  }

  /**
   * compute recommendations for a specific user based on e-commerce recommendation template's
   * standard approach.
   * If a whiteList containing outfit ids is submitted in the query, then only the outfits
   * contained in the white list will be recommended
   */
  def getRecommendations(model: ECommModel, query: Query): PredictedResult = {

    val userFeatures = model.userFeatures
    val productModels = model.productModels
    val userObjects = model.userObjects.get

    val whiteList: Option[Set[Int]] = query.whiteList

    val finalBlackList: Set[Int] = genBlackList(query = query)

    val userFeature: Option[Array[Double]] = userFeatures.get(query.user.get);

    val topScores: Array[(Int, Double)] = if (userFeature.isDefined) {

      // the user has feature vector

      // this method creates recommendations via the template's default approach
      // by calculating the score for an item w.r.t. to the query user by just
      // calculating the inner product of the users feature vector and an item's
      // feature vector
      //      logger.info("query.purchasable.isEmpty: " + query.purchasable.isEmpty)
      //      logger.info("query.purchasable.isEmpty(): " + query.purchasable.isEmpty)
      //      logger.info("query.categories.get.contains(product): " + query.categories.get.contains("product"))

      logger.info("whiteList: " + whiteList.getOrElse(Set[Int]()).size)
      if ((!query.purchasable.isEmpty && !query.purchasable.get.isEmpty()) || whiteList.size > 0) {
        logger.info("purchasable is NOT empty AND countryCode string is NOT empty")

        predictKnownUser(
          userFeature = userFeature.get,
          productModels = productModels,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList)

      } else {

        // this method uses the query user's feature vector for finding first similar
        // users to the query user and recommend then the items these users have liked      
        predictKnownUserToUser(
          userFeature = userFeature.get,
          userModels = userFeatures,
          productModels = productModels,
          userObjects = userObjects,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList)

      }
    } else {
      // the user doesn't have feature vector.
      // For example, new user is created after model is trained.
      logger.info("No userFeature found for user " + query.user.get)

      // check if the user has recent events on some items
      val recentList: Set[Int] = getRecentItems(query, 10)

      val recentFeatures: Vector[Array[Double]] = recentList.toVector
        // productModels may not contain the requested item
        .map { i =>
          productModels.get(i).flatMap { pm => pm.features }
        }.flatten

      if (recentFeatures.isEmpty) {
        logger.info(s"No features vector for recent items ${recentList}.")
        predictDefault(
          productModels = productModels,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList)
      } else {
        predictSimilarItems(
          recentFeatures = recentFeatures,
          productModels = productModels,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList)
      }
    }
    //================================================================================
    // for debugging only
    val nrPrint = Math.min(10, topScores.length)
    logger.info("printing " + nrPrint + "/" + topScores.length + " recommendations")
    for (i <- 0 to nrPrint - 1) {
      val id = topScores(i)._1
      val sc = (math floor (topScores(i)._2) * 1000.0) / 1000.0

      val tL = productModels.get(id).get.item.setTime
      val dt = new DateTime(tL)
      val dom = dt.getDayOfMonth.toString()
      val moy = dt.getMonthOfYear.toString()
      val y = dt.getYear.toString()
      val ts = y + "-" + moy + "-" + dom
      logger.info("id: " + id.toString() + " s: " + sc.toString() + " t: " + ts)
    }
    //================================================================================

    val itemScores = topScores.map {
      case (i, s) =>
        new ItemScore(
          item = i,
          score = s,
          None)
    }

    val predictedResult = new PredictedResult(itemScores)
    predictedResult
  }

  /** Generate final blackList based on other constraints */
  def genBlackList(query: Query): Set[Int] = {
    // if unseenOnly is True, get all seen items
    val seenItems: Set[Int] = if (ap.unseenOnly) {

      // get all user item events which are considered as "seen" events
      val seenEvents: Iterator[Event] = try {
        LEventStore.findByEntity(
          appName = ap.appName,
          entityType = "user",
          entityId = query.user.get.toString(),
          eventNames = Some(ap.seenEvents),
          targetEntityType = Some(Some("item")),
          // set time limit to avoid super long DB access
          timeout = Duration(200, "millis"))
      } catch {
        case e: scala.concurrent.TimeoutException =>
          logger.error(s"Timeout when read seen events." +
            s" Empty list is used. ${e}")
          Iterator[Event]()
        case e: Exception =>
          logger.error(s"Error when read seen events: ${e}")
          throw e
      }

      //      seenEvents.map { event =>
      //        try {
      //          event.targetEntityId.get.toInt
      //        } catch {
      //          case e: Throwable => {
      //            logger.error(s"Can't get targetEntityId of event ${event}.")
      //            throw e
      //          }
      //        }
      //      }.toSet

      //================================================================

      val seenEventsTargetIds = seenEvents.map {
        case event =>
          try {
            event.targetEntityId.get.toInt
          } catch {
            case e: Throwable => {
              logger.error(s"Can't get targetEntityId of event ${event}.")
              throw e
            }
          }
      }

      val seenEventsTargetIdsSet = try {
        seenEventsTargetIds.toSet
      } catch {
        case e: Throwable => {
          logger.error(".toSet method failed. Using empty set!")
          Set[Int]()
        }
      }

      seenEventsTargetIdsSet

      //================================================================      
    } else {
      Set[Int]()
    }

    // get the latest constraint unavailableItems $set event
    val unavailableItems: Set[Int] = try {
      val constr = LEventStore.findByEntity(
        appName = ap.appName,
        entityType = "constraint",
        entityId = "unavailableItems",
        eventNames = Some(Seq("$set")),
        limit = Some(1),
        latest = true,
        timeout = Duration(200, "millis"))
      if (constr.hasNext) {
        constr.next.properties.get[Set[Int]]("items")
      } else {
        Set[Int]()
      }
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read set unavailableItems event." +
          s" Empty list is used. ${e}")
        Set[Int]()
      case e: Exception =>
        logger.error(s"Error when read set unavailableItems event: ${e}")
        throw e
    }

    // combine query's blackList,seenItems and unavailableItems
    // into final blackList.
    query.blackList.getOrElse(Set[Int]()) ++ seenItems ++ unavailableItems
  }

  /**
   * Get recent events of the user on items for recommending similar items
   *  In LEventStore there are all events that have been submitted by the
   *  event client (i.e. in live setting, by back-end)
   *
   * INPUT
   * query: the query that contains information about the query user
   * limit: the number of events that should be returned
   *
   * Note: If optional parameter latest=true, then it can happen that, if
   * a user has more than limit like events since last training and these events
   * have been submitted by the event client, none of the returned liked items has
   * a feature vector, since they weren't present since last training
   */
  def getRecentItems(query: Query, limit: Int): Set[Int] = {
    // get the specified number of latest like events of query user

    logger.info("get recent items for user " + query.user.get.toString())
    val recentEvents = try {
      LEventStore.findByEntity(
        appName = ap.appName,
        // entityType and entityId is specified for fast lookup
        entityType = "user",
        entityId = query.user.get.toString(),
        //        eventNames = Some(ap.similarEvents),
        eventNames = Some(List("like")),
        targetEntityType = Some(Some("item")),
        limit = Some(limit),
        latest = true,
        // set time limit to avoid super long DB access
        timeout = Duration(200, "millis"))
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read recent events." +
          s" Empty list is used. ${e}")
        Iterator[Event]()
      case e: Exception =>
        logger.error(s"Error when read recent events: ${e}")
        throw e
    }

    //    val recentItems: Set[Int] = recentEvents.map { event =>
    //      try {
    //        event.targetEntityId.get.toInt
    //      } catch {
    //        case e: Throwable => {
    //          logger.error("Can't get targetEntityId of event ${event}.")
    //          throw e
    //        }
    //      }
    //    }.toSet

    //================================================================

    val recentItems_tmp = recentEvents.map {
      case event =>
        try {
          event.targetEntityId.get.toInt
        } catch {
          case e: Throwable => {
            logger.error(s"Can't get targetEntityId of event ${event}.")
            throw e
          }
        }
    }

    val recentItems = try {
      recentItems_tmp.toSet
    } catch {
      case e: Throwable => {
        logger.error(".toSet method failed. Using empty set in recentItems!")
        Set[Int]()
      }
    }
    //================================================================    

    logger.info("found " + recentItems.size.toString() + " recently viewed items")

    recentItems
  }

  /** Prediction for user with known feature vector */
  def predictKnownUser(
    userFeature: Array[Double],
    productModels: Map[Int, ProductModel],
    query: Query,
    whiteList: Option[Set[Int]],
    blackList: Set[Int]): Array[(Int, Double)] = {

    logger.info("method: predictKnownUser(...)")

    logger.info("product models: " + productModels.size)

    // the feature vectors for items that are product may not be defined, hence here
    // we maybe discard all product items
    val filteredForDefinedFeature = productModels.par // convert to parallel collection
      .filter {
        case (id, pm) =>
          pm.features.isDefined //&&
      }

    var startTime: Long = (new DateTime(query.startTime.getOrElse("1970-01-01T00:00:00"))).getMillis 
    if ( whiteList.nonEmpty ) {
      logger.info("set time to be 1970 time, i.e. whiteList is present")
      startTime = (new DateTime("1970-01-01T00:00:00")).getMillis
    } else {
      logger.info("whiteList is not present --> set time to be query time")
    }
    
    logger.info("features defined: " + filteredForDefinedFeature.size)
    val filteredForCandidate = filteredForDefinedFeature.par // convert to parallel collection
      .filter {
        case (id, pm) =>
          //          !pm.item.purchasable.isEmpty &&
          isCandidateItem(
            itemID = id,
            queryUserID = query.user.getOrElse(-1),
            item = pm.item,
            tstart = startTime,
            categories = query.categories,
            purchasable = query.purchasable,
            whiteList = whiteList,
            blackList = blackList)
      }

    logger.info("candidates: " + filteredForCandidate.size)
    val indexScores = filteredForCandidate.map {
      case (i, pm) =>
        // NOTE: features must be defined, so can call .get
        val s = dotProduct(userFeature, pm.features.get)
        // may customize here to further adjust score
        (i, s)
    }
      //      .filter(_._2 > 0) // only keep items with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray
    topScores
  }

  /**
   * Create recommendations on most similar users for the user in query
   *
   * WORKFLOW:
   *
   * (i)   filter all users in the model w.r.t. T (the query start time), i.e. only
   *       consider users that have like events after T
   * (ii)  get a sorted list of the users in model according to their cosine similarity
   *       with the query user, the most similar user first
   * (iii) iterate over top rated users and get num (query.num) items that
   * 			 (a) the query user hasn't seen yet (b) have been uploaded not before T
   *       (c) have at least one category of the query (d) that are in the model ...
   * (iv)  rank the items w.r.t. recently viewed items of query user (item-to-item cosine
   *       similarity) if the user recently liked some items (and these are in the model)
   *       or else, sort the items by number of likes
   *
   */
  def predictKnownUserToUser(
    userFeature: Array[Double],
    userModels: Map[Int, Array[Double]],
    productModels: Map[Int, ProductModel],
    userObjects: Map[Int, User],
    query: Query,
    whiteList: Option[Set[Int]],
    blackList: Set[Int]): Array[(Int, Double)] = {

    val queryStartTime = (new DateTime(query.startTime.getOrElse("1970-01-01T00:00:00"))).getMillis
    val startSearch = System.currentTimeMillis()
    logger.info("start time in millis: " + queryStartTime)
    logger.info("query user: " + query.user.get.toString() +
      " num: " + query.num.toString() +
      " startTime: " + query.startTime.toString())

    //================================================================================================
    //  GET THE SIMILAR USERS
    val startGetSimilarUsers = System.currentTimeMillis()
    // filter the user models to compare only the relevant users, i.e. those that have been active
    // after the start date
    logger.info("total number of users in model: " + userModels.size.toString())

    // filter the userModels that result from users that are in the trained model w.r.t. the
    // ids that are in the userObjects
    val containedUsers = userModels.par.filter { case (id, a) => userObjects.contains(id) }
    logger.info("first filtering --> containedUsers length = " + containedUsers.size.toString())
    // filter the remaining users to be the users that have liked something since queryStartTime
    val filteredUserModels = containedUsers.par.filter {
      case (id, a) => userObjects.get(id).get.lastViewEventDate.get >= queryStartTime
    }

    logger.info("remaining users after filtering for startTime: " + filteredUserModels.size.toString())

    // first find the most similar users to the user in query, therefore 
    // calculate the cosine similarity score between the user feature and the features
    // of all other users

    val userScores: Map[Int, Double] = filteredUserModels.seq
      .map { case (userid, featurevector) => (userid, cosine(featurevector, userFeature)) }
    // define the number of user from which the items are taken
    val numUsers: Int = userScores.size

    // reverse means order from highest to lowest
    // cosine similarity s is in [-1,1], meaning 1 the vectors are the same, -1 opposite
    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    // topScoresInit are the first numUsers with highest similarity to query user
    val topScoresInit = getTopN(userScores, numUsers)(ord).toArray

    // throw away the query user, topRatedUsers contains 
    val topRatedUsers = topScoresInit.filter(_._1 != query.user.get)

    val endGetSimilarUsers = System.currentTimeMillis()
    val durationGetSimilarUsers = endGetSimilarUsers - startGetSimilarUsers

    //================================================================================================
    //  FIND THE VIEWED ITEMS FOR EACH OF THE TOP RATED USERS

    var likedByUsers: Map[Int, Array[Int]] = Map.empty[Int, Array[Int]]
    var iterCount = 0
    var itemCount = 0
    val minNrItems = query.num

    var allIDs = Set.empty[Int]

    logger.info("top rated users length: " + topRatedUsers.length)
    val startWhile = System.currentTimeMillis()
    while (itemCount < minNrItems && iterCount < topRatedUsers.length) {

      val currentID = topRatedUsers(iterCount)._1
      iterCount = iterCount + 1
      //      logger.info("currentID = " + currentID)
      val seenItems: Set[Int] = {
        // get all user item events which are considered as "like" events
        val seenEvents: Iterator[Event] =
          try {
            LEventStore.findByEntity(
              appName = ap.appName,
              entityType = "user",
              entityId = currentID.toString(),
              eventNames = Some(List("like")),
              targetEntityType = Some(Some("item")),
              // set time limit to avoid super long DB access
              timeout = Duration(200, "millis"))
          } catch {
            case e: scala.concurrent.TimeoutException =>
              logger.error(s"Timeout when read seen events." +
                s" Empty list is used. ${e}")
              Iterator[Event]()
            case e: Exception =>
              logger.error(s"Error when read seen events: ${e}")
              throw e
          }

        //        seenEvents.slice(0, 200).map {
        //          case event =>
        //            try {
        //              event.targetEntityId.get.toInt
        //            } catch {
        //              case e: Throwable => {
        //                logger.error(s"Can't get targetEntityId of event ${event}.")
        //                throw e
        //              }
        //            }
        //        }.toSet

        val seenEventsTargetIds = seenEvents.slice(0, 200).map {
          case event =>
            try {
              event.targetEntityId.get.toInt
            } catch {
              case e: Throwable => {
                logger.error(s"Can't get targetEntityId of event ${event}.")
                throw e
              }
            }
        }

        val seenEventsTargetIdsSet = try {
          seenEventsTargetIds.toSet
        } catch {
          case e: Throwable => {
            logger.error(".toSet method failed. Using empty set!")
            Set[Int]()
          }
        }

        seenEventsTargetIdsSet
      } // closes seenItems: Set[Int] =

      //===========================================================================================

      //      logger.info("******************************")
      //      val tmp = seenItems.map { x => productModels.get(x).get.item.setTime }
      //      val tmp2 = tmp.toArray
      //      for ( q <- 0 to tmp2.size-1 ) {
      //        logger.info(tmp2(q).toString())
      //      }
      //      logger.info("******************************")
      //      logger.info("seenItems size = " + seenItems.size)
      //===========================================================================================      
      // filter the resulting seen items of the current similar user
      // filtering is done by isCandidateItem --> only items are considered
      // which have been uploaded since queryStartTime and which don't belong
      // to the user himself
      var filteredItems = seenItems.par.filter {
        case (id) => productModels.contains(id) &&
          productModels.get(id).isDefined &&
          productModels.get(id).exists { p => p.features.isDefined } &&
          isCandidateItem(
            itemID = id,
            queryUserID = query.user.getOrElse(-1),
            item = productModels.get(id).get.item,
            tstart = queryStartTime,
            categories = query.categories,
            purchasable = query.purchasable,
            whiteList = whiteList,
            blackList = blackList)
      }.seq

      // remove all items that have already been liked by a previous user 
      filteredItems = filteredItems -- allIDs
      // add the remaining items to the list of all items
      allIDs = allIDs union filteredItems

      itemCount = itemCount + filteredItems.size

      if (filteredItems.size > 0) {
        likedByUsers = likedByUsers.+((currentID, filteredItems.toArray))
      }
    } // end of while loop

    val endWhile = System.currentTimeMillis()
    val durationWhile = endWhile - startWhile

    logger.info("iter count: " + iterCount.toString())
    val topRatedUsersMap: Map[Int, Double] = topRatedUsers.toMap
    // Map with user id as key and a tuple ( array of views of this user, score for this user ) as values
    val topRatedUsersMapWithViews = likedByUsers.map { case (id, itemset) => (id, (itemset, topRatedUsersMap.getOrElse(id, 0.0))) }

    //================================================================================================
    //  COLLECT ALL ITEMS OF THE TOP RATED USERS IN ONE MAP
    val startAggregate = System.currentTimeMillis()
    // now aggregate the items Map[(item id, score s)]
    var itemScoreMap = Map.empty[Int, Double]
    val iter = topRatedUsersMapWithViews.iterator
    while (iter.hasNext && itemScoreMap.size < query.num) {
      val setAndScore = iter.next()._2
      val score = setAndScore._2
      val setOfIds = setAndScore._1

      val setIter = setOfIds.iterator
      while (setIter.hasNext && itemScoreMap.size < query.num) {
        val itemid = setIter.next()
        itemScoreMap = itemScoreMap.+((itemid, score))
      }
    }

    val endAggregate = System.currentTimeMillis()
    val durationAggregate = endAggregate - startAggregate

    //================================================================================================
    // FIND MOST SIMILAR ITEMS IN THE FOUND ITEMS OF SIMILAR USERS
    val startRanking = System.currentTimeMillis()
    val foundByUserSim = itemScoreMap.keySet
    val foundProductModels = productModels.filterKeys { k => foundByUserSim.contains(k) }
    logger.info("found keys of items: " + foundByUserSim.size.toString())
    logger.info("found by user PMs number: " + foundProductModels.size.toString())

    val rankedItems = rankFoundItems(query, foundProductModels, productModels)

    val endRanking = System.currentTimeMillis()
    val durationRanking = endRanking - startRanking
    //================================================================================================
    //  SORT THE REMAINING ITEMS W.R.T. THE CORRESPONDING USER SCORES AND THROW AWAY POSSIBLE DUPLICATES

    val startFinalSort = System.currentTimeMillis()
    val ordFinalMap = Ordering.by[(Int, Double), Double](_._2).reverse
    val itemScoreMapOrdered = getTopN(itemScoreMap, query.num)(ordFinalMap).toArray
    val uniqueItems = itemScoreMapOrdered.distinct
    val endFinalSort = System.currentTimeMillis()
    val durationFinalSort = endFinalSort - startFinalSort

    val finishtime = System.currentTimeMillis()

    val duration = finishtime - startSearch

    logger.info("====== TIME INFO ======")
    logger.info("duration get similar users: " + durationGetSimilarUsers.toString() + "ms")
    logger.info("duration while loop: " + durationWhile.toString() + "ms")
    logger.info("duration aggregate: " + durationAggregate.toString() + "ms")
    logger.info("duration ranking: " + durationRanking.toString() + "ms")
    logger.info("duration final sort: " + durationFinalSort.toString() + "ms")
    logger.info("overall time used: " + duration.toString() + "ms")

    rankedItems
  }

  /**
   * This method is called when we know nothing about the query user, i.e.
   * the user is not present in the model of the currently deployed engine
   * and the does not have any likes on items.
   *
   * In this case the most liked items that have been uploaded since
   * query.startTime are recommended
   */
  def predictDefault(
    productModels: Map[Int, ProductModel],
    query: Query,
    whiteList: Option[Set[Int]],
    blackList: Set[Int]): Array[(Int, Double)] = {

    val indexScores: Map[Int, Double] = productModels.par // convert back to sequential collection
      .filter {
        case (i, pm) =>
          isCandidateItem(
            itemID = i,
            queryUserID = query.user.getOrElse(-1),
            item = pm.item,
            tstart = (new DateTime(query.startTime.getOrElse("1970-01-01T00:00:00"))).getMillis,
            categories = query.categories,
            purchasable = query.purchasable,
            whiteList = whiteList,
            blackList = blackList)
      }
      .map {
        case (i, pm) =>
          // may customize here to further adjust score
          (i, pm.count.toDouble)
      }
      .seq

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    logger.info("returning " + topScores.length + " items from predictDefault method")
    topScores
  }

  /** Return top similar items based on items user recently has action on */
  def predictSimilarItems(
    recentFeatures: Vector[Array[Double]],
    productModels: Map[Int, ProductModel],
    query: Query,
    whiteList: Option[Set[Int]],
    blackList: Set[Int]): Array[(Int, Double)] = {

    logger.info("predict similar items for user " + query.user.get.toString())
    logger.info("start time: " + query.startTime.getOrElse("nothing").toString())
    val indexScores: Map[Int, Double] = productModels.par // convert to parallel collection
      .filter {
        case (i, pm) =>
          pm.features.isDefined &&
            isCandidateItem(
              itemID = i,
              queryUserID = query.user.getOrElse(-1),
              item = pm.item,
              tstart = (new DateTime(query.startTime.getOrElse("1970-01-01T00:00:00"))).getMillis,
              categories = query.categories,
              purchasable = query.purchasable,
              whiteList = whiteList,
              blackList = blackList)
      }
      .map {
        case (i, pm) =>
          val s = recentFeatures.map { rf =>
            // pm.features must be defined because of filter logic above
            cosine(rf, pm.features.get)
          }.reduce(_ + _)
          // may customize here to further adjust score
          (i, s)
      }
      //      .filter(_._2 > 0) // keep items with score > 0
      .seq // convert back to sequential collection

    // reverse means order from highest to lowest
    // cosine similarity s is in [-1,1], meaning 1 the vectors are the same, -1 opposite
    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    logger.info("returning " + topScores.length.toString() + " items")

    topScores
  }

  private def getTopN[T](s: Iterable[T], n: Int)(implicit ord: Ordering[T]): Seq[T] = {

    val q = PriorityQueue()

    for (x <- s) {
      if (q.size < n)
        q.enqueue(x)
      else {
        // q is full
        if (ord.compare(x, q.head) < 0) {
          q.dequeue()
          q.enqueue(x)
        }
      }
    }

    q.dequeueAll.toSeq.reverse
  }

  private def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var d: Double = 0
    while (i < size) {
      d += v1(i) * v2(i)
      i += 1
    }
    d
  }

  private def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var n1: Double = 0
    var n2: Double = 0
    var d: Double = 0
    while (i < size) {
      n1 += v1(i) * v1(i)
      n2 += v2(i) * v2(i)
      d += v1(i) * v2(i)
      i += 1
    }
    val n1n2 = (math.sqrt(n1) * math.sqrt(n2))
    if (n1n2 == 0) 0 else (d / n1n2)
  }

  private def isCandidateItem(
    itemID: Int,
    queryUserID: Int,
    item: Item,
    tstart: Long,
    categories: Option[Set[String]],
    purchasable: Option[String],
    whiteList: Option[Set[Int]],
    blackList: Set[Int]): Boolean = {

    // can add other custom filtering here
    //---------------------------------------------------------------------------------------
    tstart <= item.setTime && // check whether the item has been uploaded not before the desired time
      //---------------------------------------------------------------------------------------
      queryUserID != item.ownerID.getOrElse(-10000) && // check whether the item belongs to the query user, if so, do not recommend it
      //---------------------------------------------------------------------------------------
      purchasableIndicator(purchasable, item.purchasable) &&
      //---------------------------------------------------------------------------------------    
      whiteList.map(_.contains(itemID)).getOrElse(true) && // check
      //---------------------------------------------------------------------------------------    
      //      whiteListCheck(whiteList, itemID) && // check
      //---------------------------------------------------------------------------------------
//                  !blackList.contains(itemID) &&
      //---------------------------------------------------------------------------------------
      !blackListCheck(blackList, whiteList, itemID) &&
      //      ---------------------------------------------------------------------------------------
      // filter categories
      categories.map { queryCat =>
        item.categories.map { itemCat =>
          // keep this item if has overlap categories with the query
          !(itemCat.toSet.intersect(queryCat).isEmpty)
        }.getOrElse(false) // discard this item if it has no categories
      }.getOrElse(true)
  }

  private def blackListCheck(
    blackList: Set[Int],
    whiteList: Option[Set[Int]],
    itemID: Int): Boolean = {

    var retVal = false

    // if true, whiteList exists
    var wlExists: Boolean = whiteList.nonEmpty

    if (wlExists) {
      if ( blackList.contains(itemID) ) {
        retVal = false
      }
    } else {
      if (blackList.contains(itemID)) {
        retVal = true
        //      logger.info(itemID.toString() + " is in blackList")
      }
    }
    return retVal
  }

  private def whiteListCheck(whiteList: Option[Set[Int]], itemID: Int): Boolean = {

    val wl = whiteList.getOrElse(Set[Int]())

    if (wl.contains(itemID)) {
      logger.info(itemID.toString() + " is in whiteList")
    }

    return true
  }

  private def purchasableIndicator(
    queryPurchasable: Option[String],
    itemPurchasable: Option[List[String]]): Boolean = {

    //    logger.info("-----------------------------------------------------------")

    var returnValue: Boolean = false

    if (!queryPurchasable.isDefined) {
      // if the query does not contain a country code for purchasable indicator then
      // this is not required and the item is valid
      //      logger.info("purchasable is not defined")
      returnValue = true
    } else {

      if (!itemPurchasable.isDefined) {
        // if the query requests a certain country code of the item, then discard the item
        // if it does not have a purchasable country code attached
        //        logger.info("item purchasable is not defined")
        returnValue = false
      } else {
        // if the query country code is present and the item has attached country codes
        // which tell the countries where it is purchasable, then check whether the requested
        // country code is in the set of available country codes
        val itemCountryCodeSet = itemPurchasable.get.toSet
        val queryCountryCode = queryPurchasable.get

        //        if (itemCountryCodeSet.isEmpty) {
        //          logger.info("itemCountryCode is defined but empty")
        //        }

        //        logger.info("item and query purchasable defined: q=" + queryCountryCode.toString() + " i:" + itemCountryCodeSet.toSet.toString())

        if (itemCountryCodeSet.contains(queryCountryCode)) {
          //          logger.info("item country codes contain query country code: " + itemCountryCodeSet.toString() + " contains " + queryCountryCode)

          returnValue = true
        } else {
          //          logger.info("item country codes NOT contain query country code")
          returnValue = false
        }
      }
    }
    returnValue
  }

  /**
   * productModels: the map itemID -> ProductModel. the product models of the items
   * found in the loop, i.e. the product models of items to be recommended
   * allProductModels: the product models of all items in the model (since last training)
   */
  private def rankFoundItems(
    query: Query,
    productModels: Map[Int, ProductModel],
    allProductModels: Map[Int, ProductModel]): Array[(Int, Double)] = {

    val recentlyViewed: Set[Int] = getRecentItems(query, 10)
    logger.info("size of recentlyViewed: " + recentlyViewed.size.toString())

    var topScores: Array[(Int, Double)] = new Array[(Int, Double)](0)

    if (!recentlyViewed.isEmpty && !(recentlyViewed.size == 0)) {
      // if the recently viewed, i.e. liked items list is not empty       
      val recentFeatures: Vector[Array[Double]] = recentlyViewed.toVector
        // productModels may not contain the requested item
        .filter {
          case i => (allProductModels.get(i).isDefined &&
            allProductModels.get(i).get.features.isDefined)
        }
        .map {
          case j =>
            allProductModels.get(j).get.features.get
        }

      logger.info("size of productModels: " + productModels.size.toString())
      logger.info("size of recentFeatures: " + recentFeatures.size.toString())

      if (recentFeatures.size > 0) {

        val indexScores: Map[Int, Double] = productModels.par // convert to parallel collection
          .map {
            case (i, pm) =>
              val s = recentFeatures.map { rf =>
                // pm.features must be defined because of filter logic above
                cosine(rf, pm.features.get)
              }.reduce(_ + _)
              // may customize here to further adjust score
              (i, s)
          }
          .seq // convert back to sequential collection

        // reverse means order from highest to lowest
        // cosine similarity s is in [-1,1], meaning 1 the vectors are the same, -1 opposite
        val ord = Ordering.by[(Int, Double), Double](_._2).reverse
        topScores = getTopN(indexScores, query.num)(ord).toArray
      } else {
        val ts = productModels.toArray.map { case (id, pm) => (id, pm.count.toDouble) }
        val ord = Ordering.by[(Int, Double), Double](_._2).reverse
        topScores = getTopN(ts, query.num)(ord).toArray
      }
    } else {
      // if the recently liked items list is empty, then take the input items of this method
      // as output and simply transform them
      val ts = productModels.toArray.map { case (id, pm) => (id, pm.count.toDouble) }
      val ord = Ordering.by[(Int, Double), Double](_._2).reverse
      topScores = getTopN(ts, query.num)(ord).toArray
    }

    //    val nrPrint = Math.min(topScores.size, 10) - 1
    //    logger.info("topScores size = " + topScores.size.toString() + ", printing top " + (nrPrint + 1))
    //    for (i <- 0 to nrPrint) {
    //      val id = topScores(i)._1
    //      val sc = topScores(i)._2
    //      val settime = productModels.get(id).get.item.setTime
    //      val timeObj = new DateTime(settime)
    //      logger.info("id = " + id.toString() + " score = " + sc.toString() 
    //          + " upload time: " + timeObj.toString())
    //    }
    topScores
  }

  private def exportForClustering(
    tops: RDD[(Int, ProductModel)]) =
    {
      val topsArray: Array[(Int, ProductModel)] = tops.collect()
      writeTopItemsToFile(topsArray)
    }

  def writeTopItemsToFile(array: Array[(Int, ProductModel)]) {
    val writer = new BufferedWriter(new FileWriter(new File("/home/andre/RecommendationEngine/Data/Artificial/IntIds/10Users/testdata.json")))
    //    val writer = new BufferedWriter(new FileWriter(new File("/home/andre/Data/FFXData/featureVectorsForCluster.json")))

    val part1: String = "{\"event\":\"$set\",\"entityType\":\"item\",\"entityId\":\""
    val part2: String = "\",\"properties\":{\"feature\":\""
    val part3: String = "\",\"count\":\""
    val part4: String = "\"}}"

    for (i <- 0 to array.length - 1) {
      val id = array(i)._1
      val f = array(i)._2.features.get
      var fstring: String = ""
      for (j <- 0 to f.length - 1) {

        fstring = fstring + (f(j).toString())
        if (j < f.length - 1) {
          fstring = fstring + ","
        }
      }

      var cString = array(i)._2.count.toString()

      var s: String = part1 + id.toString() + part2 + fstring + part3 + cString + part4
      writer.write(s + "\n")
    }
    writer.close()
  }

  //===========================================================================================  
  //===========================================================================================
  // CLUSTER HELPER
  //===========================================================================================
  //===========================================================================================

  /**
   * returns the top n items sorted by number of likes (count parameter) that are contained in
   * the current model
   */
  private def computeTopsForClustering(model: ECommModel, daysBack: Long, maxNum: Int, minCount: Int): Array[(Int, ProductModel)] = {

    val timeBack = daysBack * 24L * 60L * 60L * 1000L
    val now: Long = System.currentTimeMillis()
    val d: Long = now - timeBack

    val pmsArray: Array[(Int, ProductModel)] = model.productModels.toArray
    val recent = pmsArray.filter { case (id, pm) => (pm.item.setTime >= d && pm.count > minCount) }

    val recentArray = recent.toArray
    logger.info("going into sort")
    // sort remaining product models w.r.t. counts
    Sorting.quickSort(recentArray)(Ordering.by[(Int, ProductModel), Int](_._2.count).reverse)
    val topArray = recentArray.slice(0, maxNum)

    topArray
  }

  /**
   * this method runs spark's MLlib k-means clustering algorithm
   * INPUT:
   * topsForClustering: the map itemID -> ProductModel for all top items
   * maxIterations: number of iterations of cluster algorithm
   * numClusters: number of clusters the top items should be divided in
   */
  private def runClusterAlgorithm(topsForClustering: Array[(Int, ProductModel)], maxIterations: Int, numClusters: Int): Array[(Int, Int)] = {

    // extract the features and the count maps
    val dataArray: Array[(Int, Array[Double])] = topsForClustering.map { case (id, pm) => (id, pm.features.get) }

    logger.info("dataArray size: " + dataArray.size.toString())

    val idArray = dataArray.map(x => x._1)
    val featureArray = dataArray.map(x => x._2)

    // get only the features as vector
    val vectorOfFeatures = featureArray.toVector

    val sc = new SparkContext()

    val featuresRDD = sc.parallelize(vectorOfFeatures.map { x => Vectors.dense(x) })

    val startKMeans = System.currentTimeMillis()

    logger.info("running KMeans with " + numClusters.toString() + " clusters and " + maxIterations.toString() + " iterations")
    val clusters = KMeans.train(featuresRDD, numClusters, maxIterations)

    val endKMeans = System.currentTimeMillis()
    val durationKMeans = endKMeans - startKMeans
    logger.info("duration of KMeans: " + durationKMeans.toString() + "ms")

    val p = clusters.predict(featuresRDD).collect()

    var itemToClusterMap: Map[Int, Int] = Map()

    for (i <- 0 to p.length - 1) {
      itemToClusterMap = itemToClusterMap.+((idArray(i), p(i) + 1))
    }

    val itemToClusterArray: Array[(Int, Int)] = itemToClusterMap.toArray

    sc.stop()

    itemToClusterArray
  }

  //  private def collectOverAllClusters(itemToClusterRDD: RDD[(Int, Int)], itemToCountRDD: RDD[(Int, Int)], query: Query): Array[ItemScore] = {
  private def collectOverAllClusters(itemToClusterArray: Array[(Int, Int)], itemToCountArray: Array[(Int, Int)], query: Query): Array[ItemScore] = {

    val totalNum = query.num

    //    val clusterIDArray = itemToClusterRDD.map { case (itemid, clusterid) => clusterid }.collect().distinct
    val clusterIDArray = itemToClusterArray.map { case (itemid, clusterid) => clusterid }.distinct
    val nrClusters = clusterIDArray.length

    //    val itemToCountMap = itemToCountRDD.collect().toMap
    val itemToCountMap = itemToCountArray.toMap
    //    val itemToClusterMap = itemToClusterRDD.collect().toMap
    val itemToClusterMap = itemToClusterArray.toMap

    logger.info("available cluster ids: ")
    for (i <- 0 to nrClusters - 1) {
      logger.info(clusterIDArray(i).toString())
    }

    val numPerCluster: Int = math.ceil(totalNum.toDouble / nrClusters.toDouble).toInt
    // for each cluster now get the n (from query) top items
    var finalIds: Array[Int] = Array.emptyIntArray
    for (i <- 0 to nrClusters - 1) {
      val clusterid = clusterIDArray(i)
      //      val currentClustersIDs = itemToClusterRDD.filter(_._2 == clusterid)
      val currentClustersIDs = itemToClusterArray.filter(_._2 == clusterid)

      logger.info("num in cluster " + clusterid.toString() + ": " + currentClustersIDs.length.toString() + "/" + numPerCluster.toString())

      // sort these items w.r.t. count and get top n
      //      val itemIdsCountArray: Array[(Int, Int)] = currentClustersIDs.collect()
      val itemIdsCountArray: Array[(Int, Int)] = currentClustersIDs
        .map { case (itemid, clusterid) => (itemid, itemToCountMap.get(itemid).get) }
      //        .map{ case ( itemid,clusterid ) => ( itemid, itemToCountRDD.filter( _._1 ) )  }

      Sorting.quickSort(itemIdsCountArray)(Ordering.by[(Int, Int), Int](_._2).reverse)
      val topArray: Array[Int] = itemIdsCountArray.slice(0, numPerCluster).map { case (id, c) => id }
      finalIds = finalIds ++ topArray
    }

    finalIds = finalIds.slice(0, totalNum)

    // if number of items is not enough, add additional items
    val idSet = finalIds.toSet.seq
    if (finalIds.length < totalNum) {
      val allIDs = itemToClusterMap.keySet
      val remainingIDs = allIDs.filterNot { x => idSet.contains(x) }.toArray

      val remainingCounts = remainingIDs.map { itemID => (itemID, itemToCountMap.get(itemID).get) }
      Sorting.quickSort(remainingCounts)(Ordering.by[(Int, Int), Int](_._2).reverse)
      val additionalCounts = remainingCounts.slice(0, totalNum - finalIds.length)

      // get the ids
      val additionalIds = additionalCounts.map(x => x._1)

      finalIds = finalIds ++ additionalIds
    }

    // mix the resulting ids randomly
    finalIds = shuffle(finalIds)

    // finally create the return object
    val itemClusters = finalIds.map {
      itemid =>
        new ItemScore(
          item = itemid,
          score = itemToCountMap.get(itemid).get.toDouble,
          cluster = Some(itemToClusterMap.get(itemid).getOrElse(0)))
    }
    itemClusters
  }

  //  def collectFromSpecificCluster(itemToClusterRDD: RDD[(Int, Int)], itemToCountRDD: RDD[(Int, Int)], query: Query): Array[ItemScore] = {
  def collectFromSpecificCluster(itemToClusterArray: Array[(Int, Int)], itemToCountArray: Array[(Int, Int)], query: Query): Array[ItemScore] = {

    val totalNum = query.num
    val returnCluster = query.returnCluster.get

    //    val itemToClusterMap = itemToClusterRDD.collect().toMap
    val itemToClusterMap = itemToClusterArray.toMap
    //    val itemToCountMap = itemToCountRDD.collect().toMap
    val itemToCountMap = itemToCountArray.toMap

    // filter all items w.r.t desired cluster number
    val clustersItems = itemToClusterMap.filter(x => x._2 == returnCluster).toArray
    logger.info("number of items in cluster " + returnCluster.toString() + ": " + clustersItems.length.toString())
    logger.info("query amount: " + totalNum.toString())
    // sort these items w.r.t. count and get top n
    val itemIdsCountArray: Array[(Int, Int)] = clustersItems
      .map { case (itemid, clusterid) => (itemid, itemToCountMap.get(itemid).get) }

    Sorting.quickSort(itemIdsCountArray)(Ordering.by[(Int, Int), Int](_._2).reverse)
    val topArray: Array[Int] = itemIdsCountArray.slice(0, totalNum).map { case (id, c) => id }

    // finally create the return object
    // finally create the return object
    val itemClusters = topArray.map {
      itemid =>
        new ItemScore(
          item = itemid,
          score = itemToCountMap.get(itemid).get.toDouble,
          cluster = Some(itemToClusterMap.get(itemid).getOrElse(0)))
    }
    itemClusters
  }

}
