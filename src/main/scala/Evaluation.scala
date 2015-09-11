package org.template.ecommercerecommendation

import io.prediction.controller.Evaluation
import io.prediction.controller.OptionAverageMetric
import io.prediction.controller.AverageMetric
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EngineParamsGenerator
import io.prediction.controller.EngineParams
import io.prediction.controller.MetricEvaluator

// usage for simple evaluation:
// pio eval org.template.ecommercerecommendation.EcommerceRecommendationEvaluation \
// org.template.ecommercerecommendation.EngineParamsList

// usage for more evaluations:
// pio eval org.template.ecommercerecommendation.ComprehensiveEcommerceRecommendationEvaluation \
// org.template.ecommercerecommendation.EngineParamsList

case class PrecisionAtK( k: Int )
  extends OptionAverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult ]
{
  require( k > 0, "k must be greater than 0" )
  
  override def header = s"Precision@K ( k=$k )"
  
  override
  def calculate( q: Query, p: PredictedResult, a: ActualResult ): Option[Double] =
  {

    val positives: Set[Int] = a.actualViews.map(  _.item ).toSet
    
    // if positives is empty, i.e. we don't have actual view events in the test
    // set, then skip this user
    if ( positives.size == 0 ) {
      return Some(111.0)
    }
    
    val predicted = p.itemScores.map { x => x.item }.toSet

    val intersectionT = predicted.intersect(positives)
        
    // count how many of the predicted items are really viewed
    val tpCount: Int = p.itemScores.take(k).filter( is => positives( is.item ) ).size
    
    // calculate and return the fraction of true positives divided by k or size of positives
//    Some(tpCount.toDouble / (math.min(k, positives.size).toDouble))
    
//    Some(tpCount.toDouble / math.min(k, positives.size))
//      Some( intersectionT.size )
    Some(intersectionT.size)
    
  }
}


object ECommerceRecommendationEvaluation extends Evaluation
{
  engineEvaluator = (
    ECommerceRecommendationEngine(),
    MetricEvaluator(
        metric = PrecisionAtK( k = 10 )
//        otherMetrics = Seq( PrecisionAtK( k=50 ), PrecisionAtK( k=100 ) ) 
    )
  )
}


object ComprehensiveECommerceRecommendationEvaluation extends Evaluation
{
  val ks = Seq( 3, 5, 10 )
  
  engineEvaluator = (
    ECommerceRecommendationEngine(),
    MetricEvaluator( 
        metric = PrecisionAtK( k = 1 ),
        otherMetrics = ( 
            ( for ( k <- ks ) yield PrecisionAtK( k = k ) )
        )
    )
  )
}


trait BaseEngineParamsList extends EngineParamsGenerator
{
  protected val baseEP = EngineParams(
    dataSourceParams = DataSourceParams( 
       appName = "EvalApp",
       startTimeTrain = "2005-11-02T09:39:45.618-08:00",
       evalParams = Some( DataSourceEvalParams( kFold = 2, queryNum = 100 ) )
    )
  )
}


object EngineParamsList extends BaseEngineParamsList
{
  engineParamsList = for (
//      rank <- Seq( 5, 10, 15 );
        rank <- Seq( 5, 10 );
//      numIterations <- Seq( 5, 10, 20 );
        regularization <- Seq( 0.01, 0.1 )
  ) yield baseEP.copy( 
    algorithmParamsList = Seq(
        ("ecomm", ECommAlgorithmParams(
                    "EvalApp",
                    false,
                    List("view"),
                    List("view"),
                    rank,
                    20,
                    regularization,
                    Some(3)
                  )
    
        ) ) )
}