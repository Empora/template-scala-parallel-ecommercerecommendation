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

import org.joda.time.DateTime

case class DataSourceEvalParams(
    kFold: Int,
    queryNum: Int
)

case class DataSourceParams(
    appName: String,            // the name from engine.json file
    startTimeTrain: String,     // the time set in engine.json file 
    evalParams: Option[DataSourceEvalParams]
) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {
    val cacheEvents = false
    
    val eventsRDD: RDD[Event] = getAllEvents( sc )
    if(cacheEvents){
      eventsRDD.cache()  
    }
    

    val minTimeTrain: Long = new DateTime(dsp.startTimeTrain).getMillis
    logger.info("Preparing training data with view events not older than " + dsp.startTimeTrain)
    
    
    
    val viewEventsRDD: RDD[ViewEvent] = getViewEvents( eventsRDD, minTimeTrain )
    val buyEventsRDD: RDD[BuyEvent] = getBuyEvents( eventsRDD, minTimeTrain )

//    logger.info("number of view events: " + viewEventsRDD.count().toString())
//    logger.info("number of buy  events: " + buyEventsRDD.count().toString())
    
    val itemSetTimes: RDD[(Int,Long)] = getItemSetTimes(sc, "item")
//    val itemSetTimes: RDD[(Int,DateTime)] = getItemSetTimes(sc, "item")
    val itemsRDD: RDD[(Int, Item)] = getItems( sc, itemSetTimes )
    
    val usersRDD: RDD[(Int, User)] = getUsers( sc )

    // uncomment this to see some information of how many events (set,view,buy,...)
    // are stored
    // CAUTION: for big data sets the execution of these few lines of code could last
    // minutes (hours maybe)
//    logger.info("readTraining:")
//    logger.info("number of item set events: " + itemSetTimes.count().toString());
   
//    logger.info("number of users      : " + usersRDD.count().toString())
//    logger.info("number of items      : " + itemsRDD.count().toString())
    
    
    if ( cacheEvents ) {
      usersRDD.cache()
      itemsRDD.cache()
      viewEventsRDD.cache()
      buyEventsRDD.cache()
    }
    
    new TrainingData(
      users = usersRDD,
      items = itemsRDD,
      viewEvents = viewEventsRDD,
      buyEvents = buyEventsRDD
    )
  }
  
  
  override
  def readEval(sc: SparkContext) 
  : Seq[ (TrainingData, EmptyEvaluationInfo, RDD[(Query,ActualResult)])] = 
  {
    require( !dsp.evalParams.isEmpty, "Must specify evalParams" )
    val evalParams = dsp.evalParams.get
    
    val kFold = evalParams.kFold

    val allEvents: RDD[Event] = getAllEvents( sc )
    
    // get the view events
    val viewEvts: RDD[ViewEvent] = getViewEvents( allEvents, 0L )
    val buyEvts: RDD[BuyEvent] = getBuyEvents( allEvents, 0L )
    val usrsRDD: RDD[(Int, User)] = getUsers( sc )
    val itmsRDD: RDD[(Int, Item)] = getItems( sc )

    
    val views: RDD[(ViewEvent, Long)] = viewEvts.zipWithUniqueId
    val buys: RDD[(BuyEvent, Long)] = buyEvts.zipWithUniqueId
    
    logger.info("views number of views = " + views.count().toString())
    logger.info("number of buys = " + buys.count().toString())
    logger.info("number of users = " + usrsRDD.count().toString())
    logger.info("number of items = " + itmsRDD.count().toString())
    
    ( 0 until kFold ).map { idx => {
      // take (kFold-1) view out kFold views as training sample
      val trainingViews = views.filter( _._2 % kFold != idx  ).map(_._1)
      val trainingBuys = buys.filter( _._2 % kFold != idx ).map( _._1 )

      // take each kFold-th view as test samples
      val testingViews = views.filter(_._2 % kFold == idx ).map(_._1)
      val testingUsers: RDD[( Int, Iterable[ViewEvent] )] = testingViews.groupBy( _.user )

      val testUserIdsRDD = testingUsers.map{ case( id, v ) => id }
      val testUserIdsArray = testUserIdsRDD.collect()
      
      ( new TrainingData( usrsRDD, itmsRDD, trainingViews, trainingBuys ),
        new EmptyEvaluationInfo(),
        testingUsers.map {
          case ( user, viewevents ) => ( Query( user, evalParams.queryNum, None, None, None, None ), 
                                         ActualResult( viewevents.toArray ) )
        }
      )
    }}
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
      eventNames = Some(List("view", "buy")),
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
  def getItemSetTimes( sc: SparkContext, entityTypeName: String ) : RDD[(Int,Long)] =
  {
      val setEventsRDD: RDD[Event] = PEventStore.find(
        appName = dsp.appName,
        entityType = Some(entityTypeName),
        eventNames = Some(List("$set"))
      )(sc)
      
//      val setTimes: RDD[(Int,DateTime)] = setEventsRDD.map { event => (event.entityId.toInt, event.eventTime) }
      val setTimes: RDD[(Int,Long)] = setEventsRDD.map { event => (event.entityId.toInt, event.eventTime.getMillis) }
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
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to ViewEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }
      .filter { _.t >= minT  }
      viewEventsRDD
  }
  
  
  /**
   * this method returns all buy events contained in allEvents
   */
  def getBuyEvents( allEvents: RDD[Event], minT: Long ): RDD[BuyEvent] =
  {
    val buyEventsRDD: RDD[BuyEvent] = allEvents
      .filter { event => event.event == "buy" }
      .map { event =>
        try {
          BuyEvent(
            user = event.entityId.toInt,
            item = event.targetEntityId.get.toInt,
            t = event.eventTime.getMillis
          )
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
   * get the users  
   */
  def getUsers(sc: SparkContext): RDD[(Int, User)] =
  {
     val cacheEvents = false
    
    // create a RDD of (entityID, User)
    val usersRDD: RDD[(Int, User)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
        User()
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId.toInt, user)
    }
    usersRDD
  }
  
  /**
   * get items from SparkContext. Each item gets attached the categories, if there are some,
   * and the time when the item has been uploaded (i.e. has been submitted by eventClient)
   */
  def getItems(sc: SparkContext, itemsSetTimes: RDD[(Int,Long)]): RDD[(Int, Item)] =
  {
      val coll = itemsSetTimes.collect()
      
      var A : Map[Int,Long] = Map()
      for ( i <- 0 to coll.length - 1 ) {
        A += ( coll(i)._1 -> coll(i)._2 )
      }
 
      val itemsRDD: RDD[(Int, Item)] = PEventStore.aggregateProperties(
        appName = dsp.appName,
        entityType = "item"
      )(sc).map { case (entityId, properties) =>
      val item = try {
        Item(categories = properties.getOpt[List[String]]("categories"),A.apply(entityId.toInt))
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
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        // Assume categories is optional property of item.
        Item(categories = properties.getOpt[List[String]]("categories"), setTime = 0L)
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

case class User()

case class Item(categories: Option[List[String]], setTime: Long)

case class ViewEvent(user: Int, item: Int, t: Long)

case class BuyEvent(user: Int, item: Int, t: Long)

class TrainingData(
  val users: RDD[(Int, User)],
  val items: RDD[(Int, Item)],
  val viewEvents: RDD[ViewEvent],
  val buyEvents: RDD[BuyEvent]
) extends Serializable {
  override def toString = {
    s"users: [${users.count()} (${users.take(2).toList}...)]" +
    s"items: [${items.count()} (${items.take(2).toList}...)]" +
    s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)" +
    s"buyEvents: [${buyEvents.count()}] (${buyEvents.take(2).toList}...)"
  }
}
