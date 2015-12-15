package org.template.classification

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

class Query(
  val ca_cdg_equipe_fez : Double,
  val ca_cdg_orgao_operacional_fez : Double,
  val ca_cdg_grupo_campanha : Double,
  val ca_cdg_categoria_prof : Double,
  val ca_hra_producoes : Double,
  val ca_nmr_dia_semana_producoes : Double,
  val ca_tpo_doacao : Double,
  val ca_tpo_cobranca : Double,
  val ca_cdg_empresa_convenio : Double,
  val ca_tpo_pessoa_contrib : Double,
  val ca_tpo_sexo_contrib : Double,
  val co_nmr_idade_contrib : Double,
  val co_nmr_meses_trabalho_prof : Double,
  val co_qtd_producoes : Double,
  val co_qtd_tipo_producoes : Double,
  val co_vlr_total_producoes : Double,
  val co_vlr_medio_producoes : Double,
  val co_nmr_meses_primeira_doacao : Double,
  val co_vlr_aceito : Double,
  val co_vlr_segmentado : Double,
  val co_nmr_duracao_ligacao : Double,
  val co_nmr_duracao_gravacao : Double,
  val co_nmr_tempo_inicio_gravacao : Double,
  val co_nmr_tempo_termino_gravacao : Double,
  val co_reputacao_prof : Double,
  val co_reputacao_contrib : Double,
  val ca_sta_resultado_monitoria : Double
) extends Serializable

class PredictedResult(
  val label: Double
) extends Serializable

object ClassificationEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map(
      	"randomforest" -> classOf[RandomForestAlgorithm]),
      classOf[Serving])
  }
}
