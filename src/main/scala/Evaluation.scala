package org.template.ecommercerecommendation

import io.prediction.controller.Evaluation
import io.prediction.controller.OptionAverageMetric
import io.prediction.controller.AverageMetric
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EngineParamsGenerator
import io.prediction.controller.EngineParams
import io.prediction.controller.MetricEvaluator

// usage:
// pio eval org.template.ecommercerecommendation.EcommerceRecommendationEvaluation \
// org.template.ecommercerecommendation.EngineParamsList


case class PrecisionAtK( k: Int ) extends OptionAverageMetric[
          EmptyEvaluationInfo, Query, PredictedResult, ActualResult ]
{
  require( k > 0, "k must be greater than 0" )
  
  override def header = s"Precision@K ( k=$k )"
  
  def calculate( q: Query, p: PredictedResult, a: ActualResult ): Option[Double] =
  {
    val positives: Set[Int] = a.actualPredictions.map(  _.item ).toSet
    
    // if positives is empty, i.e. we don't have actual view events in the test
    // set, then skip this user
    if ( positives.size == 0 ) {
      return None
    }
    
    // count how many of the predicted items are really viewed
    val tpCount: Int = p.itemScores.take(k).filter( is => positives( is.item ) ).size
    
    // calculate and return the fraction of true positives divided by k or size of positives
    Some( tpCount.toDouble / math.min(k, positives.size) )
  }
}

object ECommerceRecommendationEvaluation extends Evaluation
{
  engineEvaluator = (
    ECommerceRecommendationEngine(),
    MetricEvaluator( metric = PrecisionAtK( k = 10 ) )
  )
}


//object ComprehensiveECommerceRecommendationEvaluation extends Evaluation
//{
//  val ks = Seq(1, 3, 10)
//  
//  engineEvaluator = (
//    ECommerceRecommendationEngine(),
//    MetricEvaluator( metric = PrecisionAtK( k = 3 ) )
//  )
//}


trait BaseEngineParamsList extends EngineParamsGenerator
{
  protected val baseEP = EngineParams(
    dataSourceParams = DataSourceParams( 
       appName = "EvalApp",
       evalParams = Some( DataSourceEvalParams( kFold = 5, queryNum = 10 ) )
    )
  )
}


object EngineParamsList extends BaseEngineParamsList
{
  engineParamsList = for (
      rank <- Seq( 5, 10, 20 );
      numIterations <- Seq( 1, 5, 10 )
  ) yield baseEP.copy( 
    algorithmParamsList = Seq(
        ("ecomm", ECommAlgorithmParams(
                    "EvalApp",
                    true,
                    List("buy", "view"),
                    List("view"),
                    rank,
                    numIterations,
                    0.01,
                    Some(3)
                  )
    
        ) ) )
}