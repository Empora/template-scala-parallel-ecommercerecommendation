package org.template.ecommercerecommendation

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import io.prediction.data.storage.BiMap

import org.apache.spark.mllib.recommendation.{ Rating => MLlibRating }

@RunWith(classOf[JUnitRunner])
class TestPreventOwnOutfitsRecommendation
    extends FlatSpec with EngineTestSparkContext with Matchers {

  val algorithmParams = new ECommAlgorithmParams(
    appName = "test-app",
    unseenOnly = true,
    seenEvents = List("buy", "view", "like"),
    similarEvents = List("like"),
    rank = 10,
    numIterations = 20,
    lambda = 0.01,
    seed = Some(3))

  val algorithm = new ECommAlgorithm(algorithmParams)

  // generate some items
  val i1 = Item(categories = Some(List("outfit")), Some(4), None, None, 0)
  val i2 = Item(categories = Some(List("outfit")), Some(6), None, None, 0)
  val i3 = Item(categories = Some(List("outfit")), Some(6), None, None, 0)
  val i4 = Item(categories = Some(List("outfit")), Some(6), None, None, 0)
  val i5 = Item(categories = Some(List("outfit")), Some(6), None, None, 0)

  val items = Seq(
    1 -> i1, 2 -> i2, 3 -> i3, 4 -> i4, 5 -> i5)

  val currentTime = System.currentTimeMillis()
  val yesterday = currentTime - (24L * 60L * 60L * 1000L)
  // generate some users
  val u1 = User(Some(yesterday + 2000L))
  val u2 = User(Some(yesterday + 2000L))
  val u3 = User(Some(yesterday + 2000L))
  val u4 = User(Some(yesterday + 1000L))
  val u5 = User(Some(yesterday + 1000L))

  val users = Seq(
    1 -> u1, 2 -> u2, 3 -> u3, 4 -> u4, 5 -> u5)

  // generate some like events
  val likes = Seq(
    LikeEvent(1, 1, yesterday), LikeEvent(1, 2, yesterday + 1000L), LikeEvent(1, 3, yesterday + 2000L),
    LikeEvent(2, 1, yesterday), LikeEvent(2, 2, yesterday + 1000L), LikeEvent(2, 3, yesterday + 2000L),
    LikeEvent(3, 1, yesterday), LikeEvent(3, 2, yesterday + 1000L), LikeEvent(3, 3, yesterday + 2000L),
    LikeEvent(4, 4, yesterday), LikeEvent(4, 5, yesterday + 1000L),
    LikeEvent(5, 4, yesterday), LikeEvent(5, 5, yesterday + 1000L))

  val views = None
  val buys = None
  val dislikes = None

  "predictKnownUser method" should "return correct items taking into account owner information" in
    {

      val data = new PreparedData(
        users = sc.parallelize(users.toSeq),
        items = sc.parallelize(items.toSeq),
        viewEvents = sc.parallelize(views.toSeq),
        likeEvents = sc.parallelize(likes),
        buyEvents = sc.parallelize(buys.toSeq))

      val m = algorithm.train(sc, data)

      // create query for user 4
      val query = Query(
        method = "recommend",
        num = 5,
        user = Some(4),
        startTime = None,
        categories = None,
        purchasable = None,
        whiteList = None,
        blackList = None,
        numberOfClusters = None,
        returnCluster = None)

      val user1_Feature = m.userFeatures.get(1).get

      // manually create blacklist, the list of items the user has liked, viewed or bought
      // user 4 liked items 4 and 5
      val blacklist: Set[Int] = Set(4, 5)

      val result = algorithm.predictKnownUser(
        userFeature = user1_Feature,
        productModels = m.productModels,
        query = query,
        whiteList = None,
        blackList = blacklist)

      val recommendedIDs = result.map { case (id, score) => id }.toSet

      // user 4 liked item 4 and 5 and owns item 1, items 2 and 3 remain as recommendations
      val expected = Set(2, 3)

      recommendedIDs should equal(expected)
    }

}