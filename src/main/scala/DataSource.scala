package org.template.classification

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.storage.Storage

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import grizzled.slf4j.Logger

case class DataSourceParams(appId: Int) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {
    val eventsDb = Storage.getPEvents()
    val labeledPoints: RDD[LabeledPoint] = eventsDb.aggregateProperties(
      appId = dsp.appId,
      entityType = "auditoria",
      // only keep entities with these required properties defined
      required = Some(List( 
        "t", "ca_cdg_equipe_fez", "ca_cdg_orgao_operacional_fez", "ca_cdg_grupo_campanha", "ca_cdg_categoria_prof", "ca_hra_producoes", "ca_nmr_dia_semana_producoes", "ca_tpo_doacao", "ca_tpo_cobranca", "ca_cdg_empresa_convenio", "ca_tpo_pessoa_contrib", "ca_tpo_sexo_contrib", "co_nmr_idade_contrib", "co_nmr_meses_trabalho_prof", "co_qtd_producoes", "co_qtd_tipo_producoes", "co_vlr_total_producoes", "co_vlr_medio_producoes", "co_nmr_meses_primeira_doacao", "co_vlr_aceito", "co_vlr_segmentado", "co_nmr_duracao_ligacao", "co_nmr_duracao_gravacao", "co_nmr_tempo_inicio_gravacao", "co_nmr_tempo_termino_gravacao", "co_reputacao_prof", "co_reputacao_contrib"
      )))(sc)
      // aggregateProperties() returns RDD pair of
      // entity ID and its aggregated properties
      .map { case (entityId, properties) =>
        try {
          LabeledPoint(properties.get[Double]("ca_sta_resultado_monitoria"),
            Vectors.dense(Array(
                properties.get[Double]("ca_cdg_equipe_fez"),
                properties.get[Double]("ca_cdg_orgao_operacional_fez"),
                properties.get[Double]("ca_cdg_grupo_campanha"),
                properties.get[Double]("ca_cdg_categoria_prof"),
                properties.get[Double]("ca_hra_producoes"),
                properties.get[Double]("ca_nmr_dia_semana_producoes"),
                properties.get[Double]("ca_tpo_doacao"),
                properties.get[Double]("ca_tpo_cobranca"),
                properties.get[Double]("ca_cdg_empresa_convenio"),
                properties.get[Double]("ca_tpo_pessoa_contrib"),
                properties.get[Double]("ca_tpo_sexo_contrib"),
                properties.get[Double]("co_nmr_idade_contrib"),
                properties.get[Double]("co_nmr_meses_trabalho_prof"),
                properties.get[Double]("co_qtd_producoes"),
                properties.get[Double]("co_qtd_tipo_producoes"),
                properties.get[Double]("co_vlr_total_producoes"),
                properties.get[Double]("co_vlr_medio_producoes"),
                properties.get[Double]("co_nmr_meses_primeira_doacao"),
                properties.get[Double]("co_vlr_aceito"),
                properties.get[Double]("co_vlr_segmentado"),
                properties.get[Double]("co_nmr_duracao_ligacao"),
                properties.get[Double]("co_nmr_duracao_gravacao"),
                properties.get[Double]("co_nmr_tempo_inicio_gravacao"),
                properties.get[Double]("co_nmr_tempo_termino_gravacao"),
                properties.get[Double]("co_reputacao_prof"),
                properties.get[Double]("co_reputacao_contrib")
            ))
          )
        } catch {
          case e: Exception => {
            logger.error(s"Failed to get properties ${properties} of" +
              s" ${entityId}. Exception: ${e}.")
            throw e
          }
        }
      }.cache()

    new TrainingData(labeledPoints)
  }
}

class TrainingData(
  val labeledPoints: RDD[LabeledPoint]
) extends Serializable

