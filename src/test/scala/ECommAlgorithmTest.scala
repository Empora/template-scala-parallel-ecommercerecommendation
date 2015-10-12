package org.template.ecommercerecommendation

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
//import org.joda.time.DateTime

import io.prediction.data.storage.BiMap

import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}

@RunWith(classOf[JUnitRunner])
class ECommAlgorithmTest
  extends FlatSpec with EngineTestSparkContext with Matchers {

  val algorithmParams = new ECommAlgorithmParams(
    appName = "test-app",
    unseenOnly = true,
    seenEvents = List("buy", "view"),
    similarEvents = List("view"),
    rank = 10,
    numIterations = 20,
    lambda = 0.01,
    seed = Some(3)
  )
  val algorithm = new ECommAlgorithm(algorithmParams)

  val userStringIntMap = BiMap(Map(0 -> 0, 1 -> 1))

  val itemStringIntMap = BiMap(Map(0 -> 0, 1 -> 1, 2 -> 2))

  val users = Map(0 -> User(None), 1 -> User(None))


  val i0 = Item(categories = Some(List("c0", "c1")),0)
  val i1 = Item(categories = None,0)
  val i2 = Item(categories = Some(List("c0", "c2")),0)

  val items = Map(
    0 -> i0,
    1 -> i1,
    2 -> i2
  )

  val view = Seq(
    ViewEvent(0, 0, 1000010),
    ViewEvent(0, 1, 1000020),
    ViewEvent(0, 1, 1000020),
    ViewEvent(1, 1, 1000030),
    ViewEvent(1, 2, 1000040)
    )

  val buy = Seq(
    BuyEvent(0, 0, 1000020),
    BuyEvent(0, 1, 1000030),
    BuyEvent(1, 1, 1000040)
  )


  "ECommAlgorithm.genMLlibRating()" should "create RDD[MLlibRating] correctly" in {

    val preparedData = new PreparedData(
      users = sc.parallelize(users.toSeq),
      items = sc.parallelize(items.toSeq),
      viewEvents = sc.parallelize(view.toSeq),
      buyEvents = sc.parallelize(buy.toSeq)
    )

    val mllibRatings = algorithm.genMLlibRating(
      data = preparedData
    )

    val expected = Seq(
      MLlibRating(0, 0, 1),
      MLlibRating(0, 1, 2),
      MLlibRating(1, 1, 1),
      MLlibRating(1, 2, 1)
    )

    mllibRatings.collect should contain theSameElementsAs expected
  }

  "ECommAlgorithm.trainDefault()" should "return popular count for each item" in {
    val preparedData = new PreparedData(
      users = sc.parallelize(users.toSeq),
      items = sc.parallelize(items.toSeq),
      viewEvents = sc.parallelize(view.toSeq),
      buyEvents = sc.parallelize(buy.toSeq)
    )

    val popCount = algorithm.trainDefault(
      data = preparedData
    )

    val expected = Map(0 -> 1, 1 -> 2)

    popCount should contain theSameElementsAs expected
  }

  "ECommAlgorithm.predictKnownuser()" should "return top item" in {

      val top = algorithm.predictKnownUser(
      userFeature = Array(1.0, 2.0, 0.5),
      productModels = Map(
        0 -> ProductModel(i0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ProductModel(i1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ProductModel(i2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        method = "recom",
        num = 5,
        user = Some(0),
        startTime = None,
        categories = Some(Set("c0")),
        whiteList = None,
        blackList = None,
        numberOfClusters = None,
        returnCluster = None
      ),
        
        whiteList = None,
        blackList = Set()
    )
    
    val expected = Array((2, 7.5), (0, 5.0))
    top shouldBe expected
  }

  "ECommAlgorithm.predictDefault()" should "return top item" in {

    val top = algorithm.predictDefault(
      productModels = Map(
        0 -> ProductModel(i0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ProductModel(i1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ProductModel(i2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        method = "recom",
        num = 5,
        user = Some(0),
        startTime = None,
        categories = None,
        whiteList = None,
        blackList = None, 
        numberOfClusters = None,
        returnCluster = None
      ),
      whiteList = None,
      blackList = Set()
    )

    val expected = Array((1, 4.0), (0, 3.0), (2, 1.0))
    top shouldBe expected
  }

  "ECommAlgorithm.predictSimilar()" should "return top item" in {

    val top = algorithm.predictSimilarItems(
      recentFeatures = Vector(Array(1.0, 2.0, 0.5), Array(1.0, 0.2, 0.3)),
      productModels = Map(
        0 -> ProductModel(i0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ProductModel(i1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ProductModel(i2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        method = "recom",
        num = 5,
        user = Some(0),
        startTime = None,
        categories = Some(Set("c0")),
        whiteList = None,
        blackList = None,
        numberOfClusters = None,
        returnCluster = None
      ),
      whiteList = None,
      blackList = Set()
    )

    val expected = Array((0, 1.605), (2, 1.525))

    top(0)._1 should be (expected(0)._1)
    top(1)._1 should be (expected(1)._1)
    top(0)._2 should be (expected(0)._2 plusOrMinus 0.001)
    top(1)._2 should be (expected(1)._2 plusOrMinus 0.001)
  }
  
  "ECommAlgorithm.predictKnownUser()" should "return top items by considering upload time" in
  {
   
    // define the items
    val outfit0 = Item(categories = None, 1420070400000L) // corresponds to 2015-01-01, 00:00:00 GMT
    val outfit1 = Item(categories = None, 1388534400000L) //  corresponds to 2014-01-01, 00:00:00 GMT
    val outfit2 = Item(categories = None, 1356998400000L) //  corresponds to 2013-01-01, 00:00:00 GMT
    
    // define the map
    val items = Map(
      0 -> outfit0,
      1 -> outfit1,
      2 -> outfit2
    )
    
    val top = algorithm.predictKnownUser(
      userFeature = Array(1.0, 2.0, 0.5),
      productModels = Map(
        0 -> ProductModel(outfit0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ProductModel(outfit1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ProductModel(outfit2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        method = "recom",
        num = 5,
        user = Some(0),
        startTime = Some( "2000-01-01T12:00:00" ),
        categories = None,
        whiteList = None,
        blackList = None,
        numberOfClusters = None,
        returnCluster = None
      ),
      whiteList = None,
      blackList = Set()
    )
    
    val expected = Array(2,0,1)
    
    (top(0)._1) should be (expected(0))
    (top(1)._1) should be (expected(1))
    (top(2)._1) should be (expected(2))
    
    val top2 = algorithm.predictKnownUser(
      userFeature = Array(1.0, 2.0, 0.5),
      productModels = Map(
        0 -> ProductModel(outfit0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ProductModel(outfit1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ProductModel(outfit2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        method = "recom",
        num = 5,
        user = Some(0),
        startTime = Some( "2013-06-01T12:00:00" ),
        categories = None,
        whiteList = None,
        blackList = None,
        numberOfClusters = None,
        returnCluster = None
      ),
      whiteList = None,
      blackList = Set()
    )
    
    val expected2 = Array( 0, 1 )
    
    ( top2(0)._1 ) should be ( expected2(0) )
    ( top2(1)._1 ) should be ( expected2(1) )
    
    val top3 = algorithm.predictKnownUser(
      userFeature = Array(1.0, 2.0, 0.5),
      productModels = Map(
        0 -> ProductModel(outfit0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ProductModel(outfit1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ProductModel(outfit2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        method = "recom",
        num = 5,
        user = Some(0),
        startTime = Some( "2014-06-01T12:00:00" ),
        categories = None,
        whiteList = None,
        blackList = None,
        numberOfClusters = None,
        returnCluster = None
      ),
      whiteList = None,
      blackList = Set()
    )
    
    val expected3 = Array( 0 )
    
    ( top3(0)._1 ) should be ( expected3(0) )
  }
}
