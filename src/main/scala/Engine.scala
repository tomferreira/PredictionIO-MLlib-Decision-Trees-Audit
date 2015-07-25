package org.template.classification

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

class Query(
  val profissional : Double,
  val equipe : Double,
  val unidade : Double,
  val empresa : Double,
  val valor : Double,
  val gduracao : Double
) extends Serializable

class PredictedResult(
  val label: Double
) extends Serializable

object ClassificationEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("decisiontree" -> classOf[Algorithm]),
      classOf[Serving])
  }
}
