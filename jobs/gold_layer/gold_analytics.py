# -*- coding: utf-8 -*-
"""
Job de Análise da Camada Gold
Responsável por criar agregações e métricas de negócio para análises
Implementa as análises requeridas no desafio SGA
"""

import sys
import re
import os
from pathlib import Path
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.config import datalake_config, analytics_config
from utils.data_quality.quality_checker import DataQualityChecker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('gold_analytics.log')
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class GoldAnalyticsJob:
    """
    Job de análise para camada Gold
    Gera agregações e métricas de negócio prontas para consumo
    """

    def __init__(self):
        """Inicializa o job de análise Gold"""
        self.config = datalake_config
        self.analytics_config = analytics_config

        # Configurar paths
        self.silver_path = self.config.silver_path / "combustiveis_processed"
        self.gold_path = self.config.gold_path

        # Subdiretórios para diferentes tipos de análise
        self.analytics_path = self.gold_path / "analytics"
        self.aggregations_path = self.gold_path / "aggregations"

        # Garantir que diretórios Gold existem
        self.analytics_path.mkdir(parents=True, exist_ok=True)
        self.aggregations_path.mkdir(parents=True, exist_ok=True)

    def read_silver_data(self) -> pd.DataFrame:
        """
        Lê dados da camada Silver

        Returns:
            DataFrame consolidado da camada Silver
        """
        logger.info("Carregando dados da camada Silver")

        if not self.silver_path.exists():
            raise FileNotFoundError(f"Camada Silver não encontrada: {self.silver_path}")

        # Buscar todos os arquivos Parquet
        parquet_files = list(self.silver_path.rglob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"Nenhum arquivo Parquet encontrado em: {self.silver_path}")

        logger.info(f"Encontrados {len(parquet_files)} arquivos Silver para processar")

        # Ler e consolidar
        dataframes = []
        for file_path in parquet_files:
            try:
                df_partition = pd.read_parquet(file_path, engine="pyarrow")
                
                # Reconstruir colunas de partição
                path_parts = re.split(r'[\\/]', str(file_path.parent.relative_to(self.silver_path)))
                partition_info = {}
                for part in path_parts:
                    if '=' in part:
                        key, value = part.split('=', 1)
                        partition_info[key] = int(value) if value.isdigit() else value

                for key, value in partition_info.items():
                    df_partition[key] = value

                dataframes.append(df_partition)

            except Exception as e:
                logger.warning(f"Erro ao ler {file_path}: {str(e)}")
                continue

        if not dataframes:
            raise ValueError("Nenhum dado válido encontrado na camada Silver")

        df_consolidated = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Dados consolidados da Silver: {len(df_consolidated)} registros")

        return df_consolidated

    def generate_temporal_analytics(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Gera análises temporais (evolução de preços, sazonalidade, tendências)

        Args:
            df: DataFrame com dados Silver

        Returns:
            Dicionário com diferentes análises temporais
        """
        logger.info("Gerando análises temporais")

        analytics = {}

        # 1. Evolução mensal de preços por combustível
        monthly_evolution = df.groupby(['ano', 'mes', 'produto_normalizado']).agg({
            'Valor_Venda': ['mean', 'median', 'min', 'max', 'count'],
            'margem_percentual': ['mean', 'median']
        }).round(3)
        print(monthly_evolution.head())

        monthly_evolution.columns = ['_'.join(col).strip() for col in monthly_evolution.columns]
        monthly_evolution = monthly_evolution.reset_index()
        # Renomear colunas para o formato esperado pelo pandas
        monthly_evolution['periodo'] = pd.to_datetime(
            monthly_evolution.rename(columns={'ano': 'year', 'mes': 'month'})[['year', 'month']].assign(day=1)
        )
        
        analytics['evolucao_mensal_precos'] = monthly_evolution

        # 2. Análise de sazonalidade
        seasonality = df.groupby(['mes', 'produto_normalizado']).agg({
            'Valor_Venda': ['mean', 'std'],
            'margem_percentual': 'mean'
        }).round(3)

        seasonality.columns = ['_'.join(col).strip() for col in seasonality.columns]
        seasonality = seasonality.reset_index()

        # Calcular coeficiente de variação para identificar sazonalidade
        seasonality['coef_variacao'] = seasonality['Valor_Venda_std'] / seasonality['Valor_Venda_mean']

        analytics['sazonalidade'] = seasonality

        # 3. Tendências anuais
        yearly_trends = df.groupby(['ano', 'produto_normalizado']).agg({
            'Valor_Venda': ['mean', 'count'],
            'margem_percentual': 'mean'
        }).round(3)

        yearly_trends.columns = ['_'.join(col).strip() for col in yearly_trends.columns]
        yearly_trends = yearly_trends.reset_index()

        # Calcular crescimento ano a ano
        trends_with_growth = []
        for produto in yearly_trends['produto_normalizado'].unique():
            produto_data = yearly_trends[yearly_trends['produto_normalizado'] == produto].sort_values('ano')
            produto_data['crescimento_percentual'] = produto_data['Valor_Venda_mean'].pct_change() * 100
            trends_with_growth.append(produto_data)

        analytics['tendencias_anuais'] = pd.concat(trends_with_growth)

        # 4. Volatilidade de preços
        volatility = df.groupby(['ano', 'mes', 'produto_normalizado']).agg({
            'Valor_Venda': ['std', 'min', 'max']
        }).round(3)

        volatility.columns = ['_'.join(col).strip() for col in volatility.columns]
        volatility = volatility.reset_index()
        volatility['amplitude'] = volatility['Valor_Venda_max'] - volatility['Valor_Venda_min']
        volatility['volatilidade_relativa'] = volatility['Valor_Venda_std'] / volatility['Valor_Venda_max'] * 100

        analytics['volatilidade_precos'] = volatility

        logger.info(f"Análises temporais geradas: {len(analytics)} tabelas")
        return analytics

    def generate_regional_analytics(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Gera análises regionais (custos por região, rankings, comparações)

        Args:
            df: DataFrame com dados Silver

        Returns:
            Dicionário com análises regionais
        """
        logger.info("Gerando análises regionais")

        analytics = {}

        # 1. Ranking de preços médios por estado
        state_ranking = df.groupby(['Estado', 'produto_normalizado']).agg({
            'Valor_Venda': ['mean', 'median', 'count'],
            'margem_percentual': 'mean'
        }).round(3)

        state_ranking.columns = ['_'.join(col).strip() for col in state_ranking.columns]
        state_ranking = state_ranking.reset_index()

        # Adicionar ranking
        for produto in state_ranking['produto_normalizado'].unique():
            mask = state_ranking['produto_normalizado'] == produto
            state_ranking.loc[mask, 'ranking_preco'] = state_ranking.loc[mask, 'Valor_Venda_mean'].rank(ascending=True)

        analytics['ranking_estados'] = state_ranking

        # 2. Análise regional consolidada
        regional_analysis = df.groupby(['regiao_normalizada', 'produto_normalizado']).agg({
            'Valor_Venda': ['mean', 'median', 'std', 'count'],
            'margem_percentual': ['mean', 'std'],
            'Valor_Compra': 'mean'
        }).round(3)

        regional_analysis.columns = ['_'.join(col).strip() for col in regional_analysis.columns]
        regional_analysis = regional_analysis.reset_index()

        # Identificar região mais cara/barata por produto
        for produto in regional_analysis['produto_normalizado'].unique():
            mask = regional_analysis['produto_normalizado'] == produto
            if mask.sum() > 0:
                produto_data = regional_analysis[mask]
                min_idx = produto_data['Valor_Venda_mean'].idxmin()
                max_idx = produto_data['Valor_Venda_mean'].idxmax()

                regional_analysis.loc[min_idx, 'classificacao'] = 'MAIS_BARATA'
                regional_analysis.loc[max_idx, 'classificacao'] = 'MAIS_CARA'

        regional_analysis['classificacao'] = regional_analysis['classificacao'].fillna('INTERMEDIARIA')

        analytics['analise_regional'] = regional_analysis

        # 3. Disparidade regional
        regional_disparity = df.groupby(['regiao_normalizada', 'produto_normalizado'])['Valor_Venda'].mean().unstack(fill_value=0)

        # Calcular coeficiente de variação entre regiões
        disparity_stats = {}
        for produto in regional_disparity.columns:
            values = regional_disparity[produto][regional_disparity[produto] > 0]
            if len(values) > 1:
                disparity_stats[produto] = {
                    'coef_variacao': values.std() / values.mean() * 100,
                    'amplitude_percentual': (values.max() - values.min()) / values.min() * 100,
                    'regiao_mais_cara': values.idxmax(),
                    'regiao_mais_barata': values.idxmin()
                }

        analytics['disparidade_regional'] = pd.DataFrame(disparity_stats).T.round(3)

        logger.info(f"Análises regionais geradas: {len(analytics)} tabelas")
        return analytics

    def generate_competitive_analytics(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Gera análises competitivas (bandeiras, market share, posicionamento)

        Args:
            df: DataFrame com dados Silver

        Returns:
            Dicionário com análises competitivas
        """
        logger.info("Gerando análises competitivas")

        analytics = {}

        # 1. Market share por bandeira
        market_share = df.groupby(['bandeira_normalizada', 'produto_normalizado']).size().reset_index(name='num_pontos')

        # Calcular percentual de market share
        total_by_product = market_share.groupby('produto_normalizado')['num_pontos'].sum()
        market_share['market_share_pct'] = market_share.apply(
            lambda row: row['num_pontos'] / total_by_product[row['produto_normalizado']] * 100, 
            axis=1
        ).round(2)

        # Adicionar ranking de market share
        for produto in market_share['produto_normalizado'].unique():
            mask = market_share['produto_normalizado'] == produto
            market_share.loc[mask, 'ranking_market_share'] = market_share.loc[mask, 'market_share_pct'].rank(ascending=False)

        analytics['market_share_bandeiras'] = market_share

        # 2. Posicionamento de preços por bandeira
        brand_positioning = df.groupby(['bandeira_normalizada', 'produto_normalizado']).agg({
            'Valor_Venda': ['mean', 'std', 'count'],
            'margem_percentual': 'mean'
        }).round(3)

        brand_positioning.columns = ['_'.join(col).strip() for col in brand_positioning.columns]
        brand_positioning = brand_positioning.reset_index()

        # Classificar estratégia de preço (premium, média, econômica)
        for produto in brand_positioning['produto_normalizado'].unique():
            mask = brand_positioning['produto_normalizado'] == produto
            if mask.sum() > 0:
                produto_data = brand_positioning[mask]
                q33 = produto_data['Valor_Venda_mean'].quantile(0.33)
                q67 = produto_data['Valor_Venda_mean'].quantile(0.67)

                conditions = [
                    produto_data['Valor_Venda_mean'] <= q33,
                    produto_data['Valor_Venda_mean'] <= q67,
                    produto_data['Valor_Venda_mean'] > q67
                ]
                choices = ['ECONOMICA', 'MEDIA', 'PREMIUM']

                brand_positioning.loc[mask, 'estrategia_preco'] = np.select(conditions, choices, default='MEDIA')

        analytics['posicionamento_bandeiras'] = brand_positioning

        # 3. Análise de competitividade entre grandes bandeiras
        grandes_bandeiras = ['PETROBRAS', 'SHELL', 'IPIRANGA', 'RAIZEN']
        df_grandes = df[df['bandeira_normalizada'].isin(grandes_bandeiras)]

        if not df_grandes.empty:
            competitiveness = df_grandes.groupby(['bandeira_normalizada', 'produto_normalizado']).agg({
                'Valor_Venda': ['mean', 'count'],
                'margem_percentual': 'mean',
                'regiao_normalizada': 'nunique'  # Presença regional
            }).round(3)

            competitiveness.columns = ['_'.join(col).strip() for col in competitiveness.columns]
            competitiveness = competitiveness.reset_index()
            competitiveness.rename(columns={'regiao_normalizada_nunique': 'presenca_regional'}, inplace=True)

            analytics['competitividade_grandes_bandeiras'] = competitiveness

        logger.info(f"Análises competitivas geradas: {len(analytics)} tabelas")
        return analytics

    def generate_product_analytics(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Gera análises específicas por produto (viabilidade etanol, comparações)

        Args:
            df: DataFrame com dados Silver

        Returns:
            Dicionário com análises por produto
        """
        logger.info("Gerando análises por produto")

        analytics = {}

        # 1. Análise de viabilidade do etanol vs gasolina
        df_etanol_gasolina = df[df['produto_normalizado'].isin(['ETANOL', 'GASOLINA COMUM'])]

        if not df_etanol_gasolina.empty:
            # Agrupar por estado e período para comparação
            viabilidade_etanol = df_etanol_gasolina.groupby([
                'Estado', 'ano', 'mes', 'produto_normalizado'
            ])['Valor_Venda'].mean().unstack(fill_value=0).reset_index()

            # Calcular ratio etanol/gasolina
            if 'ETANOL' in viabilidade_etanol.columns and 'GASOLINA COMUM' in viabilidade_etanol.columns:
                viabilidade_etanol['ratio_etanol_gasolina'] = (
                    viabilidade_etanol['ETANOL'] / viabilidade_etanol['GASOLINA COMUM']
                ).round(3)

                viabilidade_etanol['etanol_viavel'] = viabilidade_etanol['ratio_etanol_gasolina'] <= self.analytics_config.viabilidade_etanol_threshold

                # Estatísticas de viabilidade por estado
                viab_stats = viabilidade_etanol.groupby('Estado').agg({
                    'ratio_etanol_gasolina': ['mean', 'min', 'max'],
                    'etanol_viavel': ['sum', 'count']
                }).round(3)

                viab_stats.columns = ['_'.join(col).strip() for col in viab_stats.columns]
                viab_stats = viab_stats.reset_index()
                viab_stats['percentual_viabilidade'] = (viab_stats['etanol_viavel_sum'] / viab_stats['etanol_viavel_count'] * 100).round(1)

                analytics['viabilidade_etanol'] = viab_stats
                analytics['historico_ratio_etanol_gasolina'] = viabilidade_etanol

        # 2. Comparação entre tipos de diesel
        df_diesel = df[df['produto_normalizado'].str.contains('DIESEL', na=False)]

        if not df_diesel.empty:
            diesel_comparison = df_diesel.groupby(['produto_normalizado', 'regiao_normalizada']).agg({
                'Valor_Venda': ['mean', 'count'],
                'margem_percentual': 'mean'
            }).round(3)

            diesel_comparison.columns = ['_'.join(col).strip() for col in diesel_comparison.columns]
            diesel_comparison = diesel_comparison.reset_index()

            analytics['comparacao_diesel'] = diesel_comparison

        # 3. Análise de produtos por categoria
        category_analysis = df.groupby(['categoria_produto', 'regiao_normalizada']).agg({
            'Valor_Venda': ['mean', 'std', 'count'],
            'margem_percentual': 'mean',
            'bandeira_normalizada': 'nunique'
        }).round(3)

        category_analysis.columns = ['_'.join(col).strip() for col in category_analysis.columns]
        category_analysis = category_analysis.reset_index()
        category_analysis.rename(columns={'bandeira_normalizada_nunique': 'num_bandeiras'}, inplace=True)

        analytics['analise_categorias'] = category_analysis

        # 4. Elasticidade de preços (correlação temporal)
        price_elasticity = []
        for produto in df['produto_normalizado'].unique():
            df_produto = df[df['produto_normalizado'] == produto]

            monthly_avg = df_produto.groupby(['ano', 'mes']).agg({
                'Valor_Venda': 'mean',
                'Valor_Compra': 'count'  # Usar como proxy de volume
            }).reset_index()

            if len(monthly_avg) > 3:  # Necessário mínimo de dados
                correlation = monthly_avg['Valor_Venda'].corr(monthly_avg['Valor_Compra'])

                price_elasticity.append({
                    'produto': produto,
                    'correlacao_preco_volume': correlation,
                    'períodos_analisados': len(monthly_avg),
                    'preco_medio': monthly_avg['Valor_Venda'].mean()
                })

        if price_elasticity:
            analytics['elasticidade_precos'] = pd.DataFrame(price_elasticity)

        logger.info(f"Análises por produto geradas: {len(analytics)} tabelas")
        return analytics

    def save_analytics(self, analytics_dict: Dict[str, Dict[str, pd.DataFrame]]) -> bool:
        """
        Salva todas as análises na camada Gold

        Args:
            analytics_dict: Dicionário com categorias de análises

        Returns:
            True se sucesso, False caso contrário
        """
        try:
            logger.info("Salvando análises na camada Gold")

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            for category, analyses in analytics_dict.items():
                category_path = self.analytics_path / category
                category_path.mkdir(parents=True, exist_ok=True)

                for analysis_name, df_analysis in analyses.items():
                    if df_analysis is not None and not df_analysis.empty:
                        file_name = f"{analysis_name}_{timestamp}.parquet"
                        file_path = category_path / file_name

                        df_analysis.to_parquet(
                            file_path,
                            compression='snappy',
                            index=False,
                            engine='pyarrow'
                        )

                        logger.info(f"Análise salva: {file_path} ({len(df_analysis)} registros)")

            return True

        except Exception as e:
            logger.error(f"Erro ao salvar análises Gold: {str(e)}")
            return False

    def execute(self) -> Dict[str, Any]:
        """
        Executa o job completo de análise Gold

        Returns:
            Relatório de execução
        """
        execution_start = datetime.now()
        logger.info("=== INICIANDO JOB GOLD ANALYTICS ===")

        try:
            # 1. Ler dados da Silver
            df_silver = self.read_silver_data()

            # 2. Gerar análises temporais
            temporal_analytics = self.generate_temporal_analytics(df_silver)

            # 3. Gerar análises regionais
            regional_analytics = self.generate_regional_analytics(df_silver)

            # 4. Gerar análises competitivas
            competitive_analytics = self.generate_competitive_analytics(df_silver)

            # 5. Gerar análises por produto
            product_analytics = self.generate_product_analytics(df_silver)

            # 6. Consolidar todas as análises
            all_analytics = {
                'temporal': temporal_analytics,
                'regional': regional_analytics,
                'competitive': competitive_analytics,
                'product': product_analytics
            }

            # 7. Salvar análises
            save_success = self.save_analytics(all_analytics)

            if not save_success:
                raise Exception("Falha ao salvar análises na camada Gold")

            # 8. Preparar relatório
            execution_end = datetime.now()
            execution_time = (execution_end - execution_start).total_seconds()

            total_analyses = sum(len(category) for category in all_analytics.values())

            execution_report = {
                "job_name": "gold_analytics",
                "execution_timestamp": execution_start.isoformat(),
                "execution_time_seconds": execution_time,
                "input_records": len(df_silver),
                "analyses_generated": total_analyses,
                "categories": list(all_analytics.keys()),
                "gold_path": str(self.analytics_path),
                "status": "success"
            }

            logger.info(f"=== JOB GOLD CONCLUÍDO COM SUCESSO ({execution_time:.1f}s) ===")
            logger.info(f"Registros processados: {len(df_silver)}")
            logger.info(f"Análises geradas: {total_analyses} em {len(all_analytics)} categorias")

            return execution_report

        except Exception as e:
            execution_end = datetime.now()
            execution_time = (execution_end - execution_start).total_seconds()

            error_report = {
                "job_name": "gold_analytics",
                "execution_timestamp": execution_start.isoformat(),
                "execution_time_seconds": execution_time,
                "status": "error",
                "error_message": str(e)
            }

            logger.error(f"=== JOB GOLD FALHOU ({execution_time:.1f}s) ===")
            logger.error(f"Erro: {str(e)}")

            return error_report

if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )

    # Executar job
    job = GoldAnalyticsJob()
    result = job.execute()

    print("\n" + "="*50)
    print("RELATÓRIO DE EXECUÇÃO - GOLD ANALYTICS")
    print("="*50)
    for key, value in result.items():
        print(f"{key}: {value}")
