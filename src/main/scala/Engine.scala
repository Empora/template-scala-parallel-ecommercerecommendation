package org.template.ecommercerecommendation

import io.prediction.controller.Engine
import io.prediction.controller.EngineFactory

case class Query(
  user: Int,
  num: Int,
  startTime: Long,
  categories: Option[Set[String]],
  whiteList: Option[Set[Int]],
  blackList: Option[Set[Int]]
) extends Serializable

case class PredictedResult(
  itemScores: Array[ItemScore]
) extends Serializable

case class ItemScore(
  item: Int,
  score: Double
) extends Serializable

case class ActualResult(
    actualViews: Array[ViewEvent]
) extends Serializable

object ECommerceRecommendationEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("ecomm" -> classOf[ECommAlgorithm]),
      classOf[Serving])
  }
}
