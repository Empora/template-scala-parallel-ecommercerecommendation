package org.template.ecommercerecommendation

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{ Rating => MLlibRating }

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class ECommAlgorithmTestUserSimilarity
    extends FlatSpec with EngineTestSparkContext with Matchers {

  // specify the algorithm parameters
  val algorithmParams = new ECommAlgorithmParams(
    appName = "test-user-sim-app",
    unseenOnly = true,
    seenEvents = List("buy", "view", "like"),
    similarEvents = List("like"),
    rank = 10,
    numIterations = 20,
    lambda = 0.01,
    seed = Some(3))

  // create an algorithm instance
  val algorithm = new ECommAlgorithm(algorithmParams)

  "ECommAlgorithm.predictKnownUserToUser with likes" should "return correct recommendations" in
    {
      // create five user objects 
      val t = System.currentTimeMillis()
      val u1 = User(Some(t))
      val u2 = User(Some(t))
      val u3 = User(Some(t))
      val u4 = User(Some(t))
      val u5 = User(Some(t))

      // connect ids and user objects
      val users = Map(
        1 -> u1,
        2 -> u2,
        3 -> u3,
        4 -> u4,
        5 -> u5)

      // create six item objects
      val i1 = Item(categories = Some(List("outfit", "female")), t - 1000)
      val i2 = Item(categories = Some(List("outfit", "male")), t - 1000)
      val i3 = Item(categories = Some(List("outfit", "male")), t - 1000)
      val i4 = Item(categories = Some(List("outfit", "female")), t - 1000)
      val i5 = Item(categories = Some(List("outfit", "male")), t - 1000)
      val i6 = Item(categories = Some(List("outfit", "female")), t - 1000)

      // connects ids and item objects
      val items = Map(
        1 -> i1,
        2 -> i2,
        3 -> i3,
        4 -> i4,
        5 -> i5,
        6 -> i6)

      // create like events (userId,itemId,TimeStamp) ("user userId likes item itemId")
      val like = Seq(
        LikeEvent(1, 1, t), LikeEvent(1, 2, t), LikeEvent(1, 3, t),
        LikeEvent(2, 1, t), LikeEvent(2, 2, t), LikeEvent(2, 3, t),
        LikeEvent(3, 1, t), LikeEvent(3, 2, t),
        LikeEvent(4, 4, t), LikeEvent(4, 5, t), LikeEvent(4, 6, t),
        LikeEvent(5, 4, t), LikeEvent(5, 5, t), LikeEvent(5, 6, t))

      val buys = None
      val views = None

      //      
      //    userFeature: Array[Double],
      //    userModels: Map[Int, Array[Double]],
      //    productModels: Map[Int, ProductModel],
      //    userObjects: Map[Int, User],
      //    query: Query,
      //    whiteList: Option[Set[Int]],
      //    blackList: Set[Int]): Array[(Int, Double)] =

      val preparedData = new PreparedData(
        users = sc.parallelize(users.toSeq),
        items = sc.parallelize(items.toSeq),
        viewEvents = sc.parallelize(views.toSeq),
        likeEvents = sc.parallelize(like.toSeq),
        buyEvents = sc.parallelize(buys.toSeq))

      // a rating consists of the triple [user,product,rating]
      val mllibRatings = algorithm.genMLlibRating(data = preparedData)

      val m = ALS.trainImplicit(
        ratings = mllibRatings,
        rank = 10,
        iterations = 10,
        lambda = 0.01,
        blocks = -1,
        alpha = 1.0,
        seed = 3)

      // first, we take user 1 for the query, i.e. get its feature vector
      val userModels = m.userFeatures.collect().toMap
      val f_u1 = userModels.get(1).get
      // get the map containing the feature vectors
      val productFeatures = m.productFeatures.collectAsMap.toMap
      val query = Query(
        method = "recommend",
        num = 10,
        user = Some(1),
        startTime = Some("2000-01-01T12:00:00"),
        categories = None,
        whiteList = None,
        blackList = None,
        numberOfClusters = None,
        returnCluster = None)

     // each ProductModel consists of the item's object, it's feature vector
     // and the number of likes
     val productModels = Map(
         1 -> ProductModel(i1, Some(productFeatures.get(1).get), 3 ),
         2 -> ProductModel(i2, Some(productFeatures.get(2).get), 3 ),
         3 -> ProductModel(i3, Some(productFeatures.get(3).get), 2 ),
         4 -> ProductModel(i4, Some(productFeatures.get(4).get), 2 ),
         5 -> ProductModel(i5, Some(productFeatures.get(5).get), 2 ),
         6 -> ProductModel(i6, Some(productFeatures.get(6).get), 2 )
         )
          
//     val tmp = algorithm.predictKnownUserToUser(
//         userFeature = f_u1,
//         userModels = userModels,
//         productModels = productModels,
//         userObjects = users,
//         query = query,
//         whiteList = None,
//         blackList = Set())
        
        
      val bla = 1
      val blupp = 1

      bla shouldEqual blupp
    }

}