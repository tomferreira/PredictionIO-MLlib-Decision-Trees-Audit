package org.template.classification

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext

import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}

import grizzled.slf4j.Logger

case class RandomForestAlgorithmParams(
  numClasses: Int,
  numTrees: Int,
  featureSubsetStrategy: String,
  impurity: String,
  maxDepth: Int,
  maxBins: Int
) extends Params

// extends P2LAlgorithm because the MLlib's RandomForestModel  doesn't contain RDD.
class RandomForestAlgorithm(val ap: RandomForestAlgorithmParams)
  extends P2LAlgorithm[PreparedData, RandomForestModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): RandomForestModel = {
    // MLLib NaiveBayes cannot handle empty training data.
    require(data.labeledPoints.take(1).nonEmpty,
      "RDD[labeledPoints] in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preparator generates PreparedData correctly.")

    // Split the data into training and test sets (20% held out for testing)
    val splits = data.labeledPoints.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))

    var categoricalFeaturesInfo = Map(
        0 -> 304, // Equipe
        1 -> 23, // Unidade
        2 -> 5, // Grupo campanha
        3 -> 16, // Categoria
        4 -> 16, // Hora producao
        5 -> 6, // Dia producao
        6 -> 5, // Tipo doacao
        7 -> 6, // Tipo de cobranca
        8 -> 66, // Empresa
        9 -> 2, // Tipo de contribuinte
        10 -> 3 // Sexo
    )
    
    val model = RandomForest.trainRegressor(
      trainingData,
      categoricalFeaturesInfo,
      ap.numTrees,
      ap.featureSubsetStrategy,
      "variance", //ap.impurity,
      ap.maxDepth,
      ap.maxBins)

    println("RandomForest")
    println("------------------------------------")

    // Evaluate model on test instances and compute test error
    val scoresAndlabels = testData.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val metrics = new BinaryClassificationMetrics(scoresAndlabels)
    println("Area under ROC = " + metrics.areaUnderROC())
    println("Area under PR = " + metrics.areaUnderPR())

    val predictionsAndlabels = testData.map { point =>
      val prediction = model.predict(point.features)
      (if (prediction <= 0.1) 0.0 else 1.0, point.label)
    }

    val metrics2 = new MulticlassMetrics(predictionsAndlabels)
    println("Recall = " + metrics2.recall(1.0)) // 1 = Fraud
    println("Precision = " + metrics2.precision(1.0)) // 1 = Fraud

    model
  }

  def predict(model: RandomForestModel, query: Query): PredictedResult = {
    val label = model.predict(Vectors.dense(
        query.ca_cdg_equipe_fez,
        query.ca_cdg_orgao_operacional_fez,
        query.ca_cdg_grupo_campanha,
        query.ca_cdg_categoria_prof,
        query.ca_hra_producoes,
        query.ca_nmr_dia_semana_producoes,
        query.ca_tpo_doacao,
        query.ca_tpo_cobranca,
        query.ca_cdg_empresa_convenio,
        query.ca_tpo_pessoa_contrib,
        query.ca_tpo_sexo_contrib,
        query.co_nmr_idade_contrib,
        query.co_nmr_meses_trabalho_prof,
        query.co_qtd_producoes,
        query.co_qtd_tipo_producoes,
        query.co_vlr_total_producoes,
        query.co_vlr_medio_producoes,
        query.co_nmr_meses_primeira_doacao,
        query.co_vlr_aceito,
        query.co_vlr_segmentado,
        query.co_nmr_duracao_ligacao,
        query.co_nmr_duracao_gravacao,
        query.co_nmr_tempo_inicio_gravacao,
        query.co_nmr_tempo_termino_gravacao,
        query.co_reputacao_prof,
        query.co_reputacao_contrib,
        query.ca_sta_resultado_monitoria
      ))
    new PredictedResult(label)
  }

}
