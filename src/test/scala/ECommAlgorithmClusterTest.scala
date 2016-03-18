package org.template.ecommercerecommendation

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

@RunWith(classOf[JUnitRunner])
class ECommAlgorithmClusterTest
    extends FlatSpec with Matchers {

  val algorithmParams = new ECommAlgorithmParams(
    appName = "test-cluster-app",
    unseenOnly = true,
    seenEvents = List("buy", "view"),
    similarEvents = List("view"),
    rank = 2,
    numIterations = 20,
    lambda = 0.01,
    seed = Some(3))

  val algorithm = new ECommAlgorithm(algorithmParams)

  // create an ECommModel

  val t: Long = System.currentTimeMillis()
  val yesterday: Long = t - (24L * 60L * 60L * 1000L)
  val oneDay: Long = 24L * 60L * 60L * 1000L

  val users = Map(1 -> User(Some(yesterday)), 2 -> User(Some(yesterday)))

  val f_u1: Array[Double] = Array(0.0, 0.0)
  val f_u2: Array[Double] = Array(0.0, 0.0)

//  Item = (
//  categories: Option[List[String]],
//  ownerID: Option[Int],
//  purchasable: Option[List[Int]],
//  relatedProducts: Option[List[Int]],
//  setTime: Long)
  
  val i1 = new Item(categories=None, ownerID=None, purchasable=None, relatedProducts=None, setTime=yesterday - oneDay)
  val i2 = new Item(categories=None, ownerID=None, purchasable=None, relatedProducts=None, setTime=yesterday - oneDay)
  val i3 = new Item(categories=None, ownerID=None, purchasable=None, relatedProducts=None, setTime=yesterday - oneDay)
  val i4 = new Item(categories=None, ownerID=None, purchasable=None, relatedProducts=None, setTime=yesterday - oneDay)

  val f_i1: Array[Double] = Array(1.0, 1.1)
  val f_i2: Array[Double] = Array(0.9, 0.9)
  val f_i3: Array[Double] = Array(-1.0, -0.9)
  val f_i4: Array[Double] = Array(-0.9, -1.9)

  val pm1: ProductModel = new ProductModel(i1, Some(f_i1), 1000)
  val pm2: ProductModel = new ProductModel(i2, Some(f_i2), 500)
  val pm3: ProductModel = new ProductModel(i3, Some(f_i3), 2000)
  val pm4: ProductModel = new ProductModel(i4, Some(f_i4), 100)

  val uFeatures = Map(1 -> f_u1, 2 -> f_u2)
  val itemToProductModel = Map(1 -> pm1, 2 -> pm2, 3 -> pm3, 4 -> pm4)

  val ecommmodel: ECommModel = new ECommModel(
    rank = 2,
    userFeatures = uFeatures,
    productModels = itemToProductModel,
    userObjects = Some(users))

//  "ECommAlgorithm.getOnboardingSuggestions()" should "return onboarding suggestions" in
//   {
//
//      val query1: Query = Query(
//        method = "cluster",
//        num = 10,
//        user = None,
//        startTime = Some("2000-01-01T12:00:00"),
//        categories = None,
//        purchasable = None,
//        whiteList = None,
//        blackList = None,
//        numberOfClusters = Some(2),
//        returnCluster = None)
//
//      val result1 = algorithm.getOnboardingSuggestions(ecommmodel, query1)
//     
////      val query2: Query = Query(
////        method = "cluster",
////        num = 10,
////        user = None,
////        startTime = Some("2000-01-01T12:00:00"),
////        categories = None,
////        purchasable = None,
////        whiteList = None,
////        blackList = None,
////        numberOfClusters = Some(2),
////        returnCluster = Some(1))
////
////      val result2 = algorithm.getOnboardingSuggestions(ecommmodel, query2)
////
////        
//      result1.itemScores should have length 4
////      result2.itemScores should have length 2
//    }

}