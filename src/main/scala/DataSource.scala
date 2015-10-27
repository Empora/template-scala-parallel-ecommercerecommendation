package org.template.ecommercerecommendation

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import grizzled.slf4j.Logger

import scala.util.Sorting

import org.joda.time.DateTime

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
    val cleanLikeevents = true

    val eventsRDD: RDD[Event] = getAllEvents(sc)
    if (cacheEvents) {
      eventsRDD.cache()
    }

    val minTimeTrain: Long = new DateTime(dsp.startTimeTrain).getMillis
    logger.info("Preparing training data with view, like and buy events not older than " + dsp.startTimeTrain)

    val viewEventsRDD: RDD[ViewEvent] = getViewEvents(eventsRDD, minTimeTrain)
    var likeEventsRDD: RDD[LikeEvent] = getLikeEvents(eventsRDD, minTimeTrain)
    val dislikeEventsRDD: RDD[DislikeEvent] = getDislikeEvents(eventsRDD, minTimeTrain)
    val buyEventsRDD: RDD[BuyEvent] = getBuyEvents(eventsRDD, minTimeTrain)

    if (cleanLikeevents) {
      likeEventsRDD = cleanLikeEvents(likeEventsRDD, dislikeEventsRDD, sc)
    }
    
    logger.info("number of like events: " + likeEventsRDD.count())
    
    val itemSetTimes: RDD[(Int, Long)] = getItemSetTimes(sc, "item")
    val itemsRDD: RDD[(Int, Item)] = getItems(sc, itemSetTimes)
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

    //    logger.info("Training data contains: ")
    //    logger.info(td.toString())

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
              case (user, viewevents) => (Query("recom", evalParams.queryNum, Some(user), None, None, None, None, None, None),
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
        .filter { event => event.event == "like" }
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

  def getDislikeEvents(allEvents: RDD[Event], minT: Long): RDD[DislikeEvent] =
    {
      val dislikeEventsRDD: RDD[DislikeEvent] = allEvents
        .filter { event => event.event == "unlike" }
        .map { event =>
          try {
            DislikeEvent(
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

      dislikeEventsRDD
    }

  /**
   * this method returns all buy events contained in allEvents
   */
  def getBuyEvents(allEvents: RDD[Event], minT: Long): RDD[BuyEvent] =
    {
      val buyEventsRDD: RDD[BuyEvent] = allEvents
        .filter { event => event.event == "buy" }
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
   * This method is responsible for deleting all like events for user id -- item id pairs
   * that are contained in the dislike events with a later time stamp than the latest
   * like event time stamp
   */
  def cleanLikeEvents(likeEvents: RDD[LikeEvent], dislikeEvents: RDD[DislikeEvent], sc: SparkContext): RDD[LikeEvent] = {

    logger.info("=========> clean like events")

    var likeArray = likeEvents.collect()
    val dislikeArray = dislikeEvents.collect()
    val dislikeTuples = dislikeArray.groupBy { x => (x.user, x.item) }.toArray
    val likeTuples = likeArray.groupBy { x => (x.user, x.item) }.toArray

    for (i <- 0 to dislikeTuples.length - 1) {

      val dislikeIDs = dislikeTuples(i)._1
      val userID = dislikeIDs._1
      val itemID = dislikeIDs._2

      // all dislike events for (uid, iid) current pair
      val dislikeEvts = dislikeTuples(i)._2

      // all like events for ( uid, iid ) current pair
      val likeEvts = likeEvents.filter { le =>
        le.user == dislikeIDs._1 && le.item == dislikeIDs._2
      }.collect()

      // sort both array according to event time
      Sorting.quickSort(dislikeEvts)(Ordering.by[(DislikeEvent), Long](_.t).reverse)
      Sorting.quickSort(likeEvts)(Ordering.by[(LikeEvent), Long](_.t).reverse)

      // compare the latest events for the current user id and the item id
      val likeIsLatest = likeEvts(0).t >= dislikeEvts(0).t
      if (likeIsLatest) {
        // do nothing, i.e. let like events untouched
      } else {
        // remove all like events for this user-item pair in like events
        likeArray = likeArray
          .filterNot { likeevent => (likeevent.user == userID && likeevent.item == itemID) }
      }
    }

    sc.parallelize(likeArray)
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
      //      val coll = itemsSetTimes.collect()

      val coll2 = itemsSetTimes.collect().toMap

      //      var A : Map[Int,Long] = Map()
      //      for ( i <- 0 to coll.length - 1 ) {
      //        A += ( coll(i)._1 -> coll(i)._2 )
      //      }

      val itemsRDD: RDD[(Int, Item)] = PEventStore.aggregateProperties(
        appName = dsp.appName,
        entityType = "item")(sc).map {
          case (entityId, properties) =>
            val item = try {
              Item(categories = properties.getOpt[List[String]]("categories"),
                ownerID = properties.getOpt[Int]("ownerID").orElse(None),
                setTime = coll2.apply(entityId.toInt))
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
case class DislikeEvent(
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
