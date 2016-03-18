package org.template.ecommercerecommendation

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import io.prediction.data.storage.BiMap

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class ECommAlgorithmTestTraining
extends FlatSpec with EngineTestSparkContext with Matchers
{

// Initialize some variables needed for tests
  
  // specify the algorithm parameters
	val algorithmParams = new ECommAlgorithmParams(
			appName = "test-app",
			unseenOnly = true,
			seenEvents = List("buy", "view", "like"),
			similarEvents = List("like"),
			rank = 10,
			numIterations = 20,
			lambda = 0.01,
    	seed = Some(3)
			)
  
  // create an algorithm instance
	val algorithm = new ECommAlgorithm(algorithmParams)

  // create the user objects
	val u1 = User( None )
	val u2 = User( None )
	val u3 = User( None )
	val u4 = User( None )
	val u5 = User( None )

  // connect ids and user objects
	val users = Map(
			1 -> u1,
			2 -> u2,
			3 -> u3,
			4 -> u4,
			5 -> u5
	)

  // create item objects
	val i1 = Item(categories = Some(List("outfit", "female")), None, None, None, 0)
	val i2 = Item(categories = Some(List("outfit", "male")), None, None, None, 0)
	val i3 = Item(categories = Some(List("outfit", "male")), None, None, None, 0)
	val i4 = Item(categories = Some(List("outfit", "female")), None, None, None, 0)
	val i5 = Item(categories = Some(List("outfit", "male")), None, None, None, 0)

  // connects ids and item objects
	val items = Map(
			1 -> i1,
			2 -> i2,
			3 -> i3,
			4 -> i4,
			5 -> i5
	)

  
  // create view events (userId,itemId,TimeStamp) ("user userId views item itemId")
	val view = Seq(
			ViewEvent(1, 2, 1000010),
			ViewEvent(1, 4, 1000020),
			ViewEvent(2, 1, 1000030),
  		ViewEvent(2, 3, 1000040),
			ViewEvent(3, 1, 1000050),
			ViewEvent(3, 4, 1000060),
			ViewEvent(3, 5, 1000070),
			ViewEvent(4, 1, 1000100),
			ViewEvent(5, 5, 1000110)
	)

	 // create like events (userId,itemId,TimeStamp) ("user userId likes item itemId")
	val like = Seq(
			LikeEvent(1, 2, 1000010),
			LikeEvent(1, 4, 1000020),
			LikeEvent(2, 1, 1000030),
  		LikeEvent(2, 3, 1000040),
			LikeEvent(3, 1, 1000050),
			LikeEvent(3, 4, 1000060),
			LikeEvent(3, 5, 1000070),
			LikeEvent(4, 1, 1000100),
			LikeEvent(5, 5, 1000110)
	)
	
  // create buy events
	val buy = None

//======================================================================================================
//======================================================================================================
  
  
  // test of method trainImplicit(). Find out if the correct user and item feature vectors
  // are returned by the algorithm
  "ECommAlgorithm.trainImplicit()" should "return correct user and item feature vectors" in {

//	  val st1: String = "ECommAlgorithmTest2 Int variant\n"
//	  val st2: String = "Test ALS.trainImplicit\n"
//	  var strarray: String = st1 + st2
//	  scala.tools.nsc.io.File("/home/andre/RecommendationEngine/Engines/TestStringInt/TestLog.txt").writeAll(strarray)

		val preparedData = new PreparedData(
				users = sc.parallelize(users.toSeq),
				items = sc.parallelize(items.toSeq),
				viewEvents = sc.parallelize(view.toSeq),
  			likeEvents = sc.parallelize(like.toSeq),
				buyEvents = sc.parallelize(buy.toSeq)
			)

	  val mllibRatings = algorithm.genMLlibRating( data = preparedData )

	  val m = ALS.trainImplicit(
			ratings = mllibRatings,
			rank = 10,
			iterations = 10,
			lambda = 0.01,
			blocks = -1,
			alpha = 1.0,
			seed  = 3
			)

      // get the learned user feature vectors from learned model m
			val userFeatures = m.userFeatures.collectAsMap.toMap

			// get the feature vectors from model
			// first, the users 
			val featureU1: Option[Array[Double]] = userFeatures.get(1)
			val featureU2: Option[Array[Double]] = userFeatures.get(2)
			val featureU3: Option[Array[Double]] = userFeatures.get(3)
			val featureU4: Option[Array[Double]] = userFeatures.get(4)
			val featureU5: Option[Array[Double]] = userFeatures.get(5)

      
      // write the vectors to log file
//			strarray = addToLog(strarray, "feature vector user u1:\n")
//			for ( i <- 0 to 9 ) {
//				val v: String = featureU1.get(i).toString() + "\n"
//				strarray = addToLog(strarray,v)
//			}
//
//			strarray =addToLog(strarray,"feature vector user u2:\n")    
//			for ( i <- 0 to 9 ) {
//				val v: String = featureU2.get(i).toString() + "\n"
//						strarray = addToLog(strarray,v)
//			}
//
//			strarray =addToLog(strarray,"feature vector user u3:\n")    
//			for ( i <- 0 to 9 ) {
//				val v: String = featureU3.get(i).toString() + "\n"
//						strarray = addToLog(strarray,v)
//			}
//
//			strarray =addToLog(strarray,"feature vector user u4:\n")    
//			for ( i <- 0 to 9 ) {
//				val v: String = featureU4.get(i).toString() + "\n"
//						strarray = addToLog(strarray,v)
//			}
//
//			strarray =addToLog(strarray,"feature vector user u5:\n")    
//			for ( i <- 0 to 9 ) {
//				val v: String = featureU5.get(i).toString() + "\n"
//						strarray = addToLog(strarray,v)
//			}

			// second, the feature vectors for the items
			val F = m.productFeatures.collectAsMap.toMap

			val featureI1: Option[Array[Double]] = F.get(1)
  		val featureI2: Option[Array[Double]] = F.get(2)
			val featureI3: Option[Array[Double]] = F.get(3)
	  	val featureI4: Option[Array[Double]] = F.get(4)
			val featureI5: Option[Array[Double]] = F.get(5)

//			strarray =addToLog(strarray,"feature vector item i1:\n")    
//			for ( i <- 0 to 9 ) {
//					val v: String = featureI1.get(i).toString() + "\n"
//					strarray = addToLog(strarray,v)
//			}
//
//			strarray =addToLog(strarray,"feature vector item i2:\n")    
//			for ( i <- 0 to 9 ) {
//			  	val v: String = featureI2.get(i).toString() + "\n"
//					strarray = addToLog(strarray,v)
//			}
//
//			strarray =addToLog(strarray,"feature vector item i3:\n")    
//			for ( i <- 0 to 9 ) {
//			  	val v: String = featureI3.get(i).toString() + "\n"
//					strarray = addToLog(strarray,v)
//			}
//
//			strarray =addToLog(strarray,"feature vector item i4:\n")    
//			for ( i <- 0 to 9 ) {
//					val v: String = featureI4.get(i).toString() + "\n"
//					strarray = addToLog(strarray,v)
//			}
//
//			strarray =addToLog(strarray,"feature vector item i5:\n")    
//			for ( i <- 0 to 9 ) {
//					val v: String = featureI5.get(i).toString() + "\n"
//					strarray = addToLog(strarray,v)
//			}
//				  scala.tools.nsc.io.File("/home/andre/RecommendationEngine/Engines/TestStringInt/TestLog.txt").writeAll(strarray)


			// SPARK 1.5.1
      // check the first and last coordinate of the feature vector of the first user
      (math floor (featureU1.get(0)*1000) )/1000 shouldBe 0.172
      (math floor (featureU1.get(9)*1000) )/1000 shouldBe 0.233

      // check the first and last coordinate of the feature vector of the last user
      (math floor (featureU5.get(0)*1000) )/1000 shouldBe -0.103
      (math floor (featureU5.get(9)*1000) )/1000 shouldBe -0.275
      
      
      // check the first and last coordinate of the feature vector of the first item
      (math floor (featureI1.get(0)*1000) )/1000 shouldBe 0.353
      (math floor (featureI1.get(9)*1000) )/1000 shouldBe 0.690

      // check the first and last coordinate of the feature vector of the last item
      (math floor (featureI5.get(0)*1000) )/1000 shouldBe -0.057
      (math floor (featureI5.get(9)*1000) )/1000 shouldBe -0.253
      
      // SPARK 1.3.1
            // check the first and last coordinate of the feature vector of the first user
//      (math floor (featureU1.get(0)*1000) )/1000 shouldBe 0.136
//      (math floor (featureU1.get(9)*1000) )/1000 shouldBe 0.210
//
//      // check the first and last coordinate of the feature vector of the last user
//      (math floor (featureU5.get(0)*1000) )/1000 shouldBe -0.102
//      (math floor (featureU5.get(9)*1000) )/1000 shouldBe -0.263
//      
//      
//      // check the first and last coordinate of the feature vector of the first item
//      (math floor (featureI1.get(0)*1000) )/1000 shouldBe 0.351
//      (math floor (featureI1.get(9)*1000) )/1000 shouldBe 0.684
//
//      // check the first and last coordinate of the feature vector of the last item
//      (math floor (featureI5.get(0)*1000) )/1000 shouldBe -0.031
//      (math floor (featureI5.get(9)*1000) )/1000 shouldBe -0.216
	}    

  
//======================================================================================================
//======================================================================================================  
  
  // test of the trainDefault method when no buy events are made
	"ECommAlgorithm.trainDefault()" should "return correct popular counts when there are no buy events" in {


		val preparedData = new PreparedData(
				users = sc.parallelize(users.toSeq),
				items = sc.parallelize(items.toSeq),
				viewEvents = sc.parallelize(view.toSeq),
				likeEvents = sc.parallelize(like.toSeq),
				buyEvents = sc.parallelize(buy.toSeq)
		)
    
		// test traindefault
		val mapPopCount = algorithm.trainDefault(preparedData)

		val i1Count = mapPopCount.get(1)
		val i2Count = mapPopCount.get(2)
		val i3Count = mapPopCount.get(3)
		val i4Count = mapPopCount.get(4)
		val i5Count = mapPopCount.get(5)

//		addToLog("i1Count = " + i1Count.toString() + "\n")
//		addToLog("i2Count = " + i2Count.toString() + "\n")
//		addToLog("i3Count = " + i3Count.toString() + "\n")
//		addToLog("i4Count = " + i4Count.toString() + "\n")
//		addToLog("i5Count = " + i5Count.toString() + "\n")  


    val expected = None
    
    i1Count shouldBe expected
    i2Count shouldBe expected
    i3Count shouldBe expected
    i4Count shouldBe expected
    i5Count shouldBe expected
	}

//======================================================================================================
//======================================================================================================

//	def addToLog(a: String,s : String): String = 
//	{
//	  var b: String= ""
//	  b = a + s
////	  scala.tools.nsc.io.File("/home/andre/RecommendationEngine/Engines/TestStringInt/TestLog.txt").writeAll(s)
//    b
//	}

}