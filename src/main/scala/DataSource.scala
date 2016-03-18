package org.template.ecommercerecommendation

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.store.PEventStore
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import grizzled.slf4j.Logger
import scala.util.Sorting
import org.joda.time.DateTime
import scala.sys.process._

import org.apache.http.client.methods.HttpDelete

case class DataSourceEvalParams(
  kFold: Int,
  queryNum: Int)

case class DataSourceParams(
  appName: String, // the name from engine.json file
  startTimeTrain: String, // the time set in engine.json file 
  evalParams: Option[DataSourceEvalParams]) extends Params

class DataSource(val dsp: DataSourceParams)
    extends PDataSource[TrainingData, EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]

  override def readTraining(sc: SparkContext): TrainingData = {
    val cacheEvents = false
    val cleanDB = true

    if (cleanDB) {
      logger.info("cleanDB = true --> start cleaning database ...")
      cleanDatabase(sc)
    } else {
      logger.info("cleanDB = false --> do not clean database.")
    }

    logger.info("get RDD of all events ...")
    val eventsRDD: RDD[Event] = getAllEvents(sc)
    if (cacheEvents) {
      eventsRDD.cache()
    }

    val minTimeTrain: Long = new DateTime(dsp.startTimeTrain).getMillis
    logger.info("Preparing training data with view, like and buy events not older than " + dsp.startTimeTrain)

    logger.info("get view events ...")
    val viewEventsRDD: RDD[ViewEvent] = getViewEvents(eventsRDD, minTimeTrain)
    logger.info("get like events ...")
    val likeEventsRDD: RDD[LikeEvent] = getLikeEvents(eventsRDD, minTimeTrain)

    //    val unlikeEventsRDD: RDD[UnlikeEvent] = getUnlikeEvents(eventsRDD, minTimeTrain)
    logger.info("get buy events ...")
    val buyEventsRDD: RDD[BuyEvent] = getBuyEvents(eventsRDD, minTimeTrain)

    logger.info("get item set times ...")
    val itemSetTimes: RDD[(Int, Long)] = getItemSetTimes(sc, "item")
    logger.info("get items ...")
    val itemsRDD: RDD[(Int, Item)] = getItems(sc, itemSetTimes)
    logger.info("get users ...")
    val usersRDD: RDD[(Int, User)] = getUsers(sc)

    if (cacheEvents) {
      usersRDD.cache()
      itemsRDD.cache()
      viewEventsRDD.cache()
      likeEventsRDD.cache()
      buyEventsRDD.cache()
    }

    val td: TrainingData = new TrainingData(
      users = usersRDD,
      items = itemsRDD,
      viewEvents = viewEventsRDD,
      likeEvents = likeEventsRDD,
      buyEvents = buyEventsRDD)

    td
  }

  override def readEval(sc: SparkContext): Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] =
    {
      require(!dsp.evalParams.isEmpty, "Must specify evalParams")
      val evalParams = dsp.evalParams.get

      val kFold = evalParams.kFold

      val allEvents: RDD[Event] = getAllEvents(sc)

      // get the view events
      val viewEvts: RDD[ViewEvent] = getViewEvents(allEvents, 0L)
      val likeEvts: RDD[LikeEvent] = getLikeEvents(allEvents, 0L)
      val buyEvts: RDD[BuyEvent] = getBuyEvents(allEvents, 0L)
      val usrsRDD: RDD[(Int, User)] = getUsers(sc)
      val itmsRDD: RDD[(Int, Item)] = getItems(sc)

      val views: RDD[(ViewEvent, Long)] = viewEvts.zipWithUniqueId
      val likes: RDD[(LikeEvent, Long)] = likeEvts.zipWithUniqueId
      val buys: RDD[(BuyEvent, Long)] = buyEvts.zipWithUniqueId

      (0 until kFold).map { idx =>
        {
          // take (kFold-1) view out kFold views as training sample
          val trainingViews = views.filter(_._2 % kFold != idx).map(_._1)
          val trainingLikes = likes.filter(_._2 % kFold != idx).map(_._1)
          val trainingBuys = buys.filter(_._2 % kFold != idx).map(_._1)

          // take each kFold-th view as test samples
          val testingViews = views.filter(_._2 % kFold == idx).map(_._1)
          val testingLikes = likes.filter(_._2 % kFold == idx).map(_._1)
          val testingUsers: RDD[(Int, Iterable[ViewEvent])] = testingViews.groupBy(_.user)

          val testUserIdsRDD = testingUsers.map { case (id, v) => id }
          val testUserIdsArray = testUserIdsRDD.collect()

          (new TrainingData(usrsRDD, itmsRDD, trainingViews, trainingLikes, trainingBuys),
            new EmptyEvaluationInfo(),
            testingUsers.map {
              case (user, viewevents) => (Query("recom", evalParams.queryNum, Some(user), None, None, None, None, None, None, None),
                ActualResult(viewevents.toArray))
            })
        }
      }
    }

  /**
   * This method searches the current SparkContext for all events specified
   * in the method, e.g. all 'view' and 'buy' events a user has made
   */
  def getAllEvents(sc: SparkContext): RDD[Event] =
    {
      val eventsRDD: RDD[Event] = PEventStore.find(
        appName = dsp.appName,
        entityType = Some("user"),
        eventNames = Some(List("view", "buy", "like", "unlike")),
        // targetEntityType is optional field of an event.
        targetEntityType = Some(Some("item")))(sc)
      eventsRDD
    }

  /**
   *  This method returns an RDD of pairs (entityID,t), which correspond to the id of the entities
   *  and the time when this entity has been submitted by eventClient
   *  For example, call this method with entityTypeName = "item" to obtain the time
   *  when the items have been uploaded
   */
  def getItemSetTimes(sc: SparkContext, entityTypeName: String): RDD[(Int, Long)] =
    {
      val setEventsRDD: RDD[Event] = PEventStore.find(
        appName = dsp.appName,
        entityType = Some(entityTypeName),
        eventNames = Some(List("$set")))(sc)

      //      val setTimes: RDD[(Int,DateTime)] = setEventsRDD.map { event => (event.entityId.toInt, event.eventTime) }
      val setTimes: RDD[(Int, Long)] = setEventsRDD.map { event =>
        (event.entityId.toInt, event.eventTime.getMillis)
      }

      setTimes
    }

  /**
   * This method gets all "view" events from all events
   */
  def getViewEvents(allEvents: RDD[Event], minT: Long): RDD[ViewEvent] =
    {
      val viewEventsRDD: RDD[ViewEvent] = allEvents
        .filter { event => event.event == "view" }
        .map { event =>
          try {
            ViewEvent(
              user = event.entityId.toInt,
              item = event.targetEntityId.get.toInt,
              t = event.eventTime.getMillis)
          } catch {
            case e: Exception =>
              logger.error(s"Cannot convert ${event} to ViewEvent." +
                s" Exception: ${e}.")
              throw e
          }
        }
        .filter { _.t >= minT }
      viewEventsRDD
    }

  /**
   * This method gets all "view" events from all events
   */
  def getLikeEvents(allEvents: RDD[Event], minT: Long): RDD[LikeEvent] =
    {
      val likeEventsRDD: RDD[LikeEvent] = allEvents
        .filter { event => event.event == "like" && event.targetEntityId.get.length() <= 10 }
        .map { event =>
          try {
            LikeEvent(
              user = event.entityId.toInt,
              item = event.targetEntityId.get.toInt,
              t = event.eventTime.getMillis)
          } catch {
            case e: Exception =>
              logger.error(s"Cannot convert ${event} to LikeEvent." +
                s" Exception: ${e}.")
              throw e
          }
        }
        .filter { _.t >= minT }
      likeEventsRDD
    }

  def getUnlikeEvents(allEvents: RDD[Event], minT: Long): RDD[UnlikeEvent] =
    {
      val unlikeEventsRDD: RDD[UnlikeEvent] = allEvents
        .filter { event => event.event == "unlike" && event.targetEntityId.get.length() <= 10 }
        .map { event =>
          try {
            UnlikeEvent(
              user = event.entityId.toInt,
              item = event.targetEntityId.get.toInt,
              t = event.eventTime.getMillis)
          } catch {
            case e: Exception =>
              logger.error(s"Cannot convert ${event} to DislikeEvent. " +
                s" Exception: ${e}.")
              throw e
          }
        }.filter { _.t >= minT }

      unlikeEventsRDD
    }

  /**
   * this method returns all buy events contained in allEvents
   */
  def getBuyEvents(allEvents: RDD[Event], minT: Long): RDD[BuyEvent] =
    {
      val buyEventsRDD: RDD[BuyEvent] = allEvents
        .filter { event => event.event == "buy" && event.targetEntityId.get.length() <= 10 }
        .map { event =>
          try {
            BuyEvent(
              user = event.entityId.toInt,
              item = event.targetEntityId.get.toInt,
              t = event.eventTime.getMillis)
          } catch {
            case e: Exception =>
              logger.error(s"Cannot convert ${event} to BuyEvent." +
                s" Exception: ${e}.")
              throw e
          }
        }.filter { _.t >= minT }
      buyEventsRDD
    }

  /**
   * This method calls the several methods to clean the database from superfluous
   * events
   */
  def cleanDatabase(sc: SparkContext) {
    logger.info("*********************** cleaning database ***********************")
//    cleanLikeEvents(sc)
    cleanItems(sc)
//    cleanPredictEvents(sc)
    logger.info("*********************** cleaning finished ***********************")
  }

  /**
   * This method removes all events with event=predict from hbase database
   * Events of this type only occur when parameter --feedback is submitted in
   * deploy command. If this parameter is not submitted, then no predict events are stored
   * an thus cleaning w.r.t. predict events can be omitted
   */
  def cleanPredictEvents(sc: SparkContext) {
    logger.info("---> clean predict events")

    // first, get all "predict" events
    val predictEvents: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("pio_pr"),
      eventNames = Some(List("predict")))(sc)

    val predictEventsArray = predictEvents.collect()

    var nr = 0
    if (predictEventsArray.length > 0) {
      var eventIDsToDelete: Array[String] = Array[String]()
      for (i <- 0 to predictEventsArray.length - 1) {
        eventIDsToDelete = eventIDsToDelete ++ predictEventsArray(i).eventId
      }
      nr = deleteEvents(eventIDsToDelete)
    }
    logger.info("removed " + nr + " predict events from data base")
  }

  /**
   * This method removes all item set events except the latest one (which is supposed
   * to contain the currently relevant properties of the item), if there are several
   * item set events at all
   */
  def cleanItems(sc: SparkContext) {
    logger.info("---> clean items")

    var totalNumberOfDeletions = 0

    // first, get all 'set item' events
    val setItemEvents: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("item"),
      eventNames = Some(List("$set")))(sc)

    // collect all set item events for each unique item
    val tooLongIds = setItemEvents.filter { x => x.entityId.length() > 10 }
    val filteredForLength = setItemEvents.filter { x => x.entityId.length() <= 10 }
//    logger.info("number of items with id out of range: " + tooLongIds.count())
    val eventIdsForDeletion = tooLongIds.map { x => x.eventId.get }.collect()
    val nrDeletedEvents = deleteEvents(eventIdsForDeletion)
    logger.info("nr of deleted items with too long int id: " + nrDeletedEvents)
        
    val groupedItems = filteredForLength.groupBy { x => x.entityId.toInt }.collect()

    logger.info("checking for deletions for " + groupedItems.length + " items")

    for (i <- 0 to groupedItems.length - 1) {
      if ((i % 100000) == 0) {
        logger.info(i + " done")
      }

      val currentSetEvents = groupedItems(i)

      val eventsArray = currentSetEvents._2.toArray

      var eventIDsToDelete: Array[String] = Array[String]()
      // if for the current id there multiple set item events, delete all but the latest
      if (eventsArray.length > 1) {

        Sorting.quickSort(eventsArray)(Ordering.by[(Event), Long](_.creationTime.getMillis).reverse)
        for (j <- 1 to eventsArray.length - 1) {
          eventIDsToDelete = eventIDsToDelete ++ eventsArray(j).eventId
        }
        val deletedEvents = deleteEvents(eventIDsToDelete)
        totalNumberOfDeletions = totalNumberOfDeletions + deletedEvents
      }
    }
    logger.info("removed " + totalNumberOfDeletions + " item set events from data base")
  }

  /**
   * This method takes all unlike events in the database, searches for all like events
   * of the same user-item-id pair and either removes all like and unlike events if the
   * unlike event was the latest event for the user-item-id pair, or removes all like and
   * unlike events for the pair except the last like event if a like event was the latest
   * event
   */
  def cleanLikeEvents(sc: SparkContext) {

    logger.info("---> clean like events")

    var nrDeletedLikes = 0
    var nrDeletedUnlikes = 0

    // all unlike events in the data base, not yet sorted w.r.t. anything
    logger.info("getting unlike events ...")
    val unlikeEvts: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("unlike")),
      targetEntityType = Some(Some("item")))(sc)

    logger.info("collect unlikes ...")
    val collectedUnlikes: Array[Event] = unlikeEvts.collect()
    logger.info("unlikeEvts map to int id ...")
    val unlikeUsers: Array[Int] = collectedUnlikes.map { x => x.entityId.toInt }
    logger.info("distinct ...")
    val unlikeUsersDistinct: Array[Int] = unlikeUsers.distinct

    // iterate over users, don't take all at once
    val nrUsers: Int = unlikeUsersDistinct.length
    val usersPerLoop: Int = 200
    val nrLoops: Int = Math.ceil(nrUsers.toDouble / usersPerLoop.toDouble).toInt

    logger.info("performing " + nrLoops + " loops")

    for (loopIdx <- 1 to nrLoops) {
      logger.info("***** loop " + loopIdx + " *****")
      val lowIdx = (loopIdx - 1) * usersPerLoop
      val highIdx = Math.min(loopIdx * usersPerLoop, nrUsers)

      logger.info("low = " + lowIdx + " high = " + highIdx)

      // take the corresponding users
      val currentDistinctUsers: Array[Int] = unlikeUsersDistinct.slice(lowIdx, highIdx)
      val nrUsersUsed = currentDistinctUsers.length

      val firstId = currentDistinctUsers(0)
      var allLikes: RDD[Event] = PEventStore.find(
        appName = dsp.appName,
        entityType = Some("user"),
        entityId = Some(firstId.toString()),
        eventNames = Some(List("like")),
        targetEntityType = Some(Some("item")))(sc)

      var rddArray = Array[RDD[Event]]()
      logger.info("getting like events for " + (nrUsersUsed) + " users")
      for (i <- 1 to nrUsersUsed - 1) {
        val id = currentDistinctUsers(i)
        val likeEvts: RDD[Event] = PEventStore.find(
          appName = dsp.appName,
          entityType = Some("user"),
          entityId = Some(id.toString()),
          eventNames = Some(List("like")),
          targetEntityType = Some(Some("item")))(sc)
        rddArray = rddArray.:+(likeEvts)
      }

      for (i <- 0 to rddArray.length - 1) {
        allLikes = sc.union(allLikes, rddArray(i))
      }

      logger.info("collect all likes events ...")
      val collectedLikes = allLikes.collect()
      logger.info("nr of collected likes: " + collectedLikes.length)

      // until here we have
      // * collectedUnlikes: Array[Event] = all unlike events in data base
      // * collectedLikes: Array[Event] = all like events of users in current loop
      // * currentDistinctUsers: Array[Int] = all user ids for which like events have been colleted
      //                         in the current loop

      var unlikeEventIDsToDelete: Vector[String] = Vector[String]() // variable to collect the event ids of events to delete
      var likeEventIDsToDelete: Vector[String] = Vector[String]()

      for (i <- 0 to currentDistinctUsers.length - 1) {

        val userId = currentDistinctUsers(i)
        // (i) get all unlike events of the user
        val unlikeEventsOfUser: Array[Event] = collectedUnlikes.filter { e => e.entityId.toInt == userId }

        for (j <- 0 to unlikeEventsOfUser.length - 1) {
          val currentUnlikeEvent = unlikeEventsOfUser(j)
          val itemId = currentUnlikeEvent.targetEntityId.get.toInt
          val eventId = currentUnlikeEvent.eventId.get.toString()
          val unlikeEventTime = currentUnlikeEvent.eventTime.getMillis

          // collect the unlike event's id since this has to be deleted in every case
          unlikeEventIDsToDelete = unlikeEventIDsToDelete.:+(eventId)
          // collect all like events for same user and target item id
          val correspondingLikes = collectedLikes.filter { e =>
            (e.entityId.toInt == userId && e.targetEntityId.get.toInt == itemId)
          }
          // extract the relevant information
          val mappedLikes: Array[(String, Long)] = correspondingLikes.map { e => (e.eventId.get, e.eventTime.getMillis) }

          if (mappedLikes.length > 0) {
            Sorting.quickSort(mappedLikes)(Ordering.by[(String, Long), Long](_._2).reverse)
            if (mappedLikes(0)._2 > unlikeEventTime) {
              for (k <- 1 to mappedLikes.length - 1) {
                likeEventIDsToDelete = likeEventIDsToDelete.:+(mappedLikes(k)._1)
              }
            } else {
              for (k <- 0 to mappedLikes.length - 1) {
                likeEventIDsToDelete = likeEventIDsToDelete.:+(mappedLikes(k)._1)
              }
            }
          }
        }
      } // end of loop for all currentDistinct users

      logger.info("nr of unlike events to delete: " + unlikeEventIDsToDelete.length)
      logger.info("nr of like events to delete: " + likeEventIDsToDelete.length)

      val nrUnlikesDeleted = deleteEvents(unlikeEventIDsToDelete.toArray)
      logger.info("deleted " + nrUnlikesDeleted + " unlike events")
      val nrLikesDeleted = deleteEvents(likeEventIDsToDelete.toArray)
      logger.info("deleted " + nrLikesDeleted + " like events")

      nrDeletedLikes = nrDeletedLikes + nrLikesDeleted
      nrDeletedUnlikes = nrDeletedUnlikes + nrUnlikesDeleted
    }

    logger.info("total number of deleted like events: " + nrDeletedLikes)
    logger.info("total number of deleted unlike events: " + nrDeletedUnlikes)
  }

  /**
   * This method takes a list of event ids and removes the corresponding events from database
   */
  def deleteEvents(idList: Array[String]): Int =
  {
      // perform the deletions by executing the following curl command via http client
      // curl -i -X DELETE http://localhost:7070/events/1234.json?accessKey=eE7OVB

      // ClientTestApp (local)    
      // val accesskey = "eE7OVBlxLknmVC7UHV0jwtAwWP8BDsfMWe23Ey9eJtEb5EQJJyqVEQjW3a3IHFhS"
      // FFXApp (dev)
       val accesskey = "lnerkAS2WHiLkcR90SZ8dWY99WPLAz93VioxQ3BoqVZALfntUp6NQd3DP1gm1alY"
      // FFXRecommender (live)
      // val accesskey = "iamMrxYuq2b2EVu9D0dKN4MfhVGhqB4V7bIDValJIBd8DE5pw1h84A47sKQpdQTz"
       
      val str1 = "http://localhost:7070/events/"
      val str2 = ".json?accessKey=" + accesskey

      var nr200 = 0
      var nrElse = 0
      val client = new DefaultHttpClient
      for (i <- 0 to idList.length - 1) {
        val url = str1 + idList(i) + str2
        val del = new HttpDelete(url)
        val response = client.execute(del)
        val code = response.getStatusLine.getStatusCode

        val inputStream = response.getEntity.getContent
        inputStream.close()

        if (code == 200) {
          nr200 = nr200 + 1
        } else {
          nrElse = nrElse + 1
        }
      }

      val all = nr200 + nrElse
      val nr = nr200
      nr
    }

  /**
   * get the users. Unlike in the original template, where the EventStore was searched
   * for user set events, here all view events are considered and for each user id
   * these events are aggregated to obtain a map userID --> latestViewEvent time stamp.
   * This is then stored in a user object. Thus we are able to know when the user's
   * last view event happened, i.e. in a sense if the user is an active user
   */
  def getUsers(sc: SparkContext): RDD[(Int, User)] =
    {
      val cacheEvents = false

      // search the event store for view events and keep the user ids and the time stamp
      // of the view event
      val v: RDD[(Int, Long)] = PEventStore.find(
        appName = dsp.appName,
        entityType = Some("user"),
        eventNames = Some(List("like")))(sc).map { case likeevent => (likeevent.entityId.toInt, likeevent.eventTime.getMillis) }

      // get an RDD containing unique ids and the corresponding time stamps
      val aggregatedDates = v.groupByKey

      // keep only the lastest view event times
      val maxDates = aggregatedDates.map { case (id, iter) => (id, iter.toArray.reduceLeft(_ max _)) }

      val datesMap = maxDates.collectAsMap()

      val datesArray = datesMap.toArray

      // create an array of ids and User objects containing the corresponding time stamp
      val usersArray = datesArray.map {
        case (id, t) =>
          (id, User(Some(t)))
      }

      val usersRDD = sc.parallelize(usersArray)

      usersRDD
    }

  /**
   * get items from SparkContext. Each item gets attached the categories, if there are some,
   * and the time when the item has been uploaded (i.e. has been submitted by eventClient)
   */
  def getItems(sc: SparkContext, itemsSetTimes: RDD[(Int, Long)]): RDD[(Int, Item)] =
    {
      val coll = itemsSetTimes.collect()

      var A: Map[Int, Long] = Map()
      for (i <- 0 to coll.length - 1) {
        A += (coll(i)._1 -> coll(i)._2)
      }

      val itemsRDD: RDD[(Int, Item)] = PEventStore.aggregateProperties(
        appName = dsp.appName,
        entityType = "item")(sc).map {
          case (entityId, properties) =>
            val item = try {
              Item(categories = properties.getOpt[List[String]]("categories"),
                ownerID = properties.getOpt[Int]("ownerID").orElse(None),
                purchasable = properties.getOpt[List[String]]("purchasable"),
//                purchasable = None,
//                relatedProducts = properties.getOpt[List[Int]]("relatedProducts"),
                relatedProducts = None,
                setTime = A.getOrElse(entityId.toInt, 0L))
            } catch {
              case e: Exception => {
                logger.error(s"Failed to get properties ${properties} of" + s" item ${entityId}. Exception: ${e}.")
                throw e
              }
            }
            (entityId.toInt, item)
        }
      itemsRDD
    }

  /**
   * Default method to get items. The items do not get attached any time information.
   */
  def getItems(sc: SparkContext): RDD[(Int, Item)] =
    {
      val itemsRDD: RDD[(Int, Item)] = PEventStore.aggregateProperties(
        appName = dsp.appName,
        entityType = "item")(sc).map {
          case (entityId, properties) =>
            val item = try {
              // Assume categories is optional property of item.
              Item(categories = properties.getOpt[List[String]]("categories"),
                ownerID = properties.getOpt[Int]("ownerID").orElse(None),
                purchasable = properties.getOpt[List[String]]("purchasable"),
                relatedProducts = properties.getOpt[List[Int]]("relatedProducts"),
                setTime = 0L)
            } catch {
              case e: Exception => {
                logger.error(s"Failed to get properties ${properties} of" +
                  s" item ${entityId}. Exception: ${e}.")
                throw e
              }
            }
            (entityId.toInt, item)
        }
      itemsRDD
    }
}

case class User(
  lastViewEventDate: Option[Long] // time stamp of the latest view event of the user
  )

// object of type item
//  * optionally contains a list of categories ("male","female","outfit","product")
//  * contains the time stamp indicating when the item has been uploaded
case class Item(
  categories: Option[List[String]],
  ownerID: Option[Int],
  purchasable: Option[List[String]],
  relatedProducts: Option[List[Int]],
  setTime: Long)

/**
 * object of class ViewEvent contains
 * the user id
 * the item id
 * the time stamp
 * and represents the event of when the user viewed the item,
 * e.g. liked it (he has seen it), got recommended it
 */
case class ViewEvent(
  user: Int,
  item: Int,
  t: Long)

/**
 * object of class ViewEvent contains
 * the user id
 * the item id
 * the time stamp
 * and represents the event of when the user bought the item
 */
case class BuyEvent(
  user: Int,
  item: Int,
  t: Long)

/**
 * object of class LikeEvent contains
 * the user id
 * the item id
 * the time stamp
 * and represents the event of when the user liked (favs) an item
 */
case class LikeEvent(
  user: Int,
  item: Int,
  t: Long)

/**
 * object of class DislikeEvent contains
 * the user id
 * the item id
 * the time stamp
 * and represents the event of when the user disliked (unfavs) an item
 */
case class UnlikeEvent(
  user: Int,
  item: Int,
  t: Long)

class TrainingData(
    val users: RDD[(Int, User)],
    val items: RDD[(Int, Item)],
    val viewEvents: RDD[ViewEvent],
    val likeEvents: RDD[LikeEvent],
    val buyEvents: RDD[BuyEvent]) extends Serializable {
  override def toString = {
    s"users: [${users.count()} (${users.take(2).toList}...)]" +
      s"items: [${items.count()} (${items.take(2).toList}...)]" +
      s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)" +
      s"likeEvents: [${likeEvents.count()}] (${likeEvents.take(2).toList}...)" +
      s"buyEvents: [${buyEvents.count()}] (${buyEvents.take(2).toList}...)"
  }
}
