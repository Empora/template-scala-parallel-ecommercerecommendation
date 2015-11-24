package org.template.ecommercerecommendation

import io.prediction.controller.Engine
import io.prediction.controller.EngineFactory

import org.joda.time.DateTime

case class Query(
  // "recommend" or "cluster" for either the template's standard recommendation or the on-boarding
  // item suggestions based on the clustering result
  method: String,
  // the number of returned items, recommended items or suggested on-boarding items, respectively
  num: Int,
  // the id of the user for which recommendations should be made, optional, only needed for method
  // "recommend"
  user: Option[Int],
  // time stamp in format "yyyy-mm-ddThh:mm:ss" (e.g. 2015-06-01T12:34:21), optional, only needed
  // for method "recommend", if submitted only items that have been uploaded since
  // (and not before) are recommended
  startTime: Option[String],
  // a list of castegories (e.g. ["male","product"]), optional, only needed for method "recommend",
  // only items which belong to at least one of the categories in the list are recommended
  categories: Option[Set[String]],
  // a list of country codes ( e.g. ["DE","EN"] ). Used for filtering recommendations of items
  // of category product, i.e. one wants to get recommended items that are products and that
  // are purchasable in a certain country
  purchasable: Option[String],
  // a list of item ids, optional, only needed for method "recommend", items that are contained in
  // the white list are always recommended
  whiteList: Option[Set[Int]],
  // a list of item ids, optional, only needed for method "recommend", items that are contained in
  // the black list are sorted out and get not recommended
  blackList: Option[Set[Int]],
  // the number of clusters which should be trained by spark's MLlib k-means clustering, optional,
  // only needed for method "cluster", if not specified, default is set to 5
  numberOfClusters: Option[Int],
  // the number of the cluster for which item suggestions should be returned, optional, only needed
  // for method "cluster", if not specified items from all clusters are suggested
  returnCluster: Option[Int]
) extends Serializable

/**
 * class objects of PredictedResult type contain the results for both, "recommend" and "cluster"
 */
case class PredictedResult(
  itemScores: Array[ItemScore]
) extends Serializable

/**
 *  An instance of this class represents one returned item and contains
 *  item - the id of the item 
 *  score - recommend: the score for score item, cluster: the number of likes of this item
 *  cluster - optional: the id of the cluster this item belongs to 
 */
case class ItemScore(
  item: Int,
  score: Double,
  cluster: Option[Int]
) extends Serializable

/**
 * only needed on evaluation framework (pio eval). Doesn't work for this template at the moment
 */
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
