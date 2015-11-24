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

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{
  HBaseAdmin,
  HTable,
  Put,
  Get,
  Delete,
  Result,
  Scan,
  ResultScanner
}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HTableDescriptor

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
    val clean_database = false

    if (clean_database) {
      cleanDatabase(sc)
    }

    val eventsRDD: RDD[Event] = getAllEvents(sc)
    if (cacheEvents) {
      eventsRDD.cache()
    }

    val minTimeTrain: Long = new DateTime(dsp.startTimeTrain).getMillis
    logger.info("Preparing training data with view, like and buy events not older than " + dsp.startTimeTrain)

    val viewEventsRDD: RDD[ViewEvent] = getViewEvents(eventsRDD, minTimeTrain)
    val likeEventsRDD: RDD[LikeEvent] = getLikeEvents(eventsRDD, minTimeTrain)
    val unlikeEventsRDD: RDD[UnlikeEvent] = getUnlikeEvents(eventsRDD, minTimeTrain)

    val buyEventsRDD: RDD[BuyEvent] = getBuyEvents(eventsRDD, minTimeTrain)

    val itemSetTimes: RDD[(Int, Long)] = getItemSetTimes(sc, "item")
    val itemsRDD: RDD[(Int, Item)] = getItems(sc, itemSetTimes)
    val usersRDD: RDD[(Int, User)] = getUsers(sc)

    // print items
    //    val ITEMS = itemsRDD.collect()
    //    for ( i <- 0 to ITEMS.length - 1 ) {
    //      val it = ITEMS(i)
    //      val id = it._1
    //      val obj = it._2
    //      logger.info("item " + id + ", category: " + obj.categories.get.toString()
    //           + ", purchasable: " + obj.purchasable.getOrElse(None)
    //           + ", relatedProducts: " + obj.relatedProducts.getOrElse(None)
    //           + ", setTime: " + obj.setTime )
    //    }
    //    

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

    //    val itemsArray = itemsRDD.collect()
    //    logger.info("nr items: " + itemsRDD.count())
    //    for ( i <- 0 to itemsArray.length-1 ) {
    //      
    //      val itemID = itemsArray(i)._1
    //      val itemObject = itemsArray(i)._2
    //      
    //      logger.info("id: " + itemID.toString() )
    //      logger.info("cats: " + itemObject.categories.toString() + " t=" + itemObject.setTime )
    //      
    //    }    
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

  def getUnlikeEvents(allEvents: RDD[Event], minT: Long): RDD[UnlikeEvent] =
    {
      val unlikeEventsRDD: RDD[UnlikeEvent] = allEvents
        .filter { event => event.event == "unlike" }
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
   * This method calls the several methods to clean the database from superfluous
   * events
   */
  def cleanDatabase(sc: SparkContext) {
    logger.info("******** cleaning database ********")

    //    val conf = HBaseConfiguration.create()
    //    val hbadmin = new HBaseAdmin(conf)
    //    
    //    val tableNames = hbadmin.getTableNames
    //    
    //    val bla = hbadmin.listTableNames()
    //      
    //    for ( i <- 0 to bla.length-1 ) {
    //      logger.info(bla(i).toString())
    //    }
    //    
    //    val td: HTableDescriptor = hbadmin.getTableDescriptor(bla(0))
    //    
    //    val t = new HTable(conf,bla(0).toBytes())
    //    

    cleanLikeEvents(sc)
    //    cleanItems(sc)
    //    cleanPredictEvents(sc)
  }

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
    // first, get all 'set item' events
    val setItemEvents: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("item"),
      eventNames = Some(List("$set")))(sc)

    // collect all set item events for each unique item
    val groupedItems = setItemEvents.groupBy { x => x.entityId.toInt }.collect()

    logger.info("checking for deletions for " + groupedItems.length + " item set events")

    var eventIDsToDelete: Array[String] = Array[String]()
    for (i <- 0 to groupedItems.length - 1) {

      if ((i % 1000) == 0) {
        logger.info(i + " done")
      }

      val currentSetEvents = groupedItems(i)

      // if for the current id there multiple set item events, delete all but the latest
      if (currentSetEvents._2.size > 1) {
        val eventsArray = currentSetEvents._2.toArray
        Sorting.quickSort(eventsArray)(Ordering.by[(Event), Long](_.eventTime.getMillis).reverse)

        for (j <- 1 to eventsArray.length - 1) {
          eventIDsToDelete = eventIDsToDelete ++ eventsArray(j).eventId
        }
      }
    }

    val deletedEvents = deleteEvents(eventIDsToDelete)
    logger.info("removed " + deletedEvents + " item set events from data base")
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
    // first get all "unlike" events that are in the database
    logger.info("getting unlike events")
    val unlikeEvts: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      targetEntityType = Some(Some("item")),
      eventNames = Some(List("unlike")))(sc)

    logger.info("getting like events")
    val likeEvts: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      targetEntityType = Some(Some("item")),
      eventNames = Some(List("like")))(sc)

    val uniqueUnlikeIDsArray: Array[Int] = unlikeEvts.map { x => x.entityId.toInt }.distinct().collect()
    logger.info("found unlike events for " + uniqueUnlikeIDsArray.length + " user ids")
    if (uniqueUnlikeIDsArray.length > 0) {

      logger.info("filter like events w.r.t. unlike user ids")
      val filteredLikes: RDD[Event] = likeEvts.filter { event => uniqueUnlikeIDsArray contains event.entityId.toInt }

      logger.info("collect the events, unlike ...")
      val unlikeEventsArray: Array[Event] = unlikeEvts.collect()
      logger.info("like ...")
      val filteredLikesArray: Array[Event] = filteredLikes.collect()

      var eventIDsToDelete: Array[String] = Array[String]() // variable to collect the event ids of events to delete
      for (i <- 0 to uniqueUnlikeIDsArray.length - 1) {
        logger.info("unlike user " + (i + 1) + ": " + uniqueUnlikeIDsArray(i))
        val currentUser: Int = uniqueUnlikeIDsArray(i)
        // get the unlike events of this user
        val usersUnlikeEvts: Array[Event] = unlikeEventsArray.filter { e => e.entityId.toInt == currentUser }
        val usersLikeEvts: Array[Event] = filteredLikesArray.filter { e => e.entityId.toInt == currentUser }

        logger.info("current # unlikes: " + usersUnlikeEvts.length)
        logger.info("current # likes: " + usersLikeEvts.length)

        // now, for all unlike events, get the like events with same target entity id
        for (j <- 0 to usersUnlikeEvts.length - 1) {
          // get the id of the item of the j-th unlike event
          val targetItem = usersUnlikeEvts(j).targetEntityId.get.toInt
          val targetsUnlikeEvents = usersUnlikeEvts.filter { e =>
            e.targetEntityId.get.toInt == targetItem
          }
          val correspondingLikeEvents = usersLikeEvts.filter { e =>
            e.targetEntityId.get.toInt == targetItem
          }

          if (correspondingLikeEvents.length > 0) {

            Sorting.quickSort(targetsUnlikeEvents)(Ordering.by[(Event), Long](_.eventTime.getMillis).reverse)
            Sorting.quickSort(correspondingLikeEvents)(Ordering.by[(Event), Long](_.eventTime.getMillis).reverse)

            if (correspondingLikeEvents(0).eventTime.getMillis >= targetsUnlikeEvents(0).eventTime.getMillis) {
              // if the latest event is a like event, then do not delete the latest like
              // event but all other like events
              for (t <- 1 to correspondingLikeEvents.length - 1) {
                logger.info("add like event to delete")
                eventIDsToDelete = eventIDsToDelete ++ (correspondingLikeEvents(t).eventId)
              }
            } else {
              // if the latest event is an unlike event, then delete all like events
              for (t <- 0 to correspondingLikeEvents.length - 1) {
                logger.info("add like event to delete")
                eventIDsToDelete = eventIDsToDelete ++ (correspondingLikeEvents(t).eventId)
              }
            }
            
          }
        }
        // finally add the unlike events
        for (j <- 0 to usersUnlikeEvts.length - 1) {
          logger.info("add unlike event to delete")
          eventIDsToDelete = eventIDsToDelete ++ (usersUnlikeEvts(j).eventId)
        }
      } // end for uniqueUnlikeIDsArray

      
      logger.info("deleting like and unlike events ...")
      val nr = deleteEvents(eventIDsToDelete)
      logger.info("removed " + nr + " like/unlike events from data base")
    } else {
      logger.info("not enough unlike events to justify starting expensive cleaning operation!")
      logger.info("Maybe next time ;-)")
    }
  }

  /**
   * This method takes a list of event ids and removes the corresponding events from database
   */
  def deleteEvents(idList: Array[String]): Int =
    {
      val str1 = "curl -i -X DELETE http://localhost:7070/events/"

      // str2 for local ClientTestApp
//            val str2 = ".json?accessKey=eE7OVBlxLknmVC7UHV0jwtAwWP8BDsfMWe23Ey9eJtEb5EQJJyqVEQjW3a3IHFhS"
//       str2 for dev FFXApp
      val str2 = ".json?accessKey=lnerkAS2WHiLkcR90SZ8dWY99WPLAz93VioxQ3BoqVZALfntUp6NQd3DP1gm1alY"
      // str2 for live FFXRecommender
//                val str2 = ".json?accessKey=iamMrxYuq2b2EVu9D0dKN4MfhVGhqB4V7bIDValJIBd8DE5pw1h84A47sKQpdQTz"

      var nr = 0

      for (i <- 0 to idList.length - 1) {
        val cmd: String = str1 + idList(i) + str2
        val result = { cmd !! }
        nr = nr + 1
      }
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
                relatedProducts = properties.getOpt[List[Int]]("relatedProducts"),
                setTime = A.getOrElse(entityId.toInt, System.currentTimeMillis()))
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
