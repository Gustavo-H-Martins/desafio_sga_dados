# -*- coding: utf-8 -*-
"""
Job de Transformação da Camada Silver
Responsável por limpar, normalizar e enriquecer dados da camada Bronze
Implementa transformações de negócio e padronizações
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.config import datalake_config, quality_config, analytics_config
from utils.data_quality.quality_checker import DataQualityChecker

logger = logging.getLogger(__name__)

class SilverTransformationJob:
    """
    Job de transformação para camada Silver
    Aplica limpeza, normalização e enriquecimento dos dados
    """

    def __init__(self):
        """Inicializa o job de transformação Silver"""
        self.config = datalake_config
        self.quality_config = quality_config
        self.analytics_config = analytics_config
        self.quality_checker = DataQualityChecker(quality_config)

        # Configurar paths
        self.bronze_path = self.config.bronze_path / "combustiveis"
        self.silver_path = self.config.silver_path / "combustiveis_processed"

        # Garantir que diretório Silver existe
        self.silver_path.mkdir(parents=True, exist_ok=True)

    def read_bronze_data(self) -> pd.DataFrame:
        """
        Lê dados da camada Bronze

        Returns:
            DataFrame consolidado da camada Bronze
        """
        logger.info("Carregando dados da camada Bronze")

        if not self.bronze_path.exists():
            raise FileNotFoundError(f"Camada Bronze não encontrada: {self.bronze_path}")

        # Buscar todos os arquivos Parquet nas partições
        parquet_files = list(self.bronze_path.rglob("*.parquet"))

        if not parquet_files:
            raise FileNotFoundError(f"Nenhum arquivo Parquet encontrado em: {self.bronze_path}")

        logger.info(f"Encontrados {len(parquet_files)} arquivos Parquet para processar")

        # Ler e consolidar todos os arquivos
        dataframes = []
        for file_path in parquet_files:
            try:
                df_partition = pd.read_parquet(file_path)

                # Reconstruir colunas de partição a partir do path
                parts = file_path.parent.name.split('=')
                if len(parts) >= 2:
                    # Assumir estrutura ano=YYYY/mes=MM
                    partition_info = {}
                    path_parts = str(file_path.parent.relative_to(self.bronze_path)).split('/')
                    for part in path_parts:
                        if '=' in part:
                            key, value = part.split('=', 1)
                            partition_info[key] = int(value) if value.isdigit() else value

                    # Adicionar colunas de partição ao DataFrame
                    for key, value in partition_info.items():
                        df_partition[key] = value

                dataframes.append(df_partition)

            except Exception as e:
                logger.warning(f"Erro ao ler {file_path}: {str(e)}")
                continue

        if not dataframes:
            raise ValueError("Nenhum dado válido encontrado na camada Bronze")

        df_consolidated = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Dados consolidados da Bronze: {len(df_consolidated)} registros")

        return df_consolidated

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica limpeza básica nos dados

        Args:
            df: DataFrame para limpar

        Returns:
            DataFrame limpo
        """
        logger.info("Aplicando limpeza de dados")

        df_clean = df.copy()

        # 1. Remover duplicatas baseadas em chaves de negócio
        business_keys = ["CNPJ", "Data_Coleta", "Produto"]
        existing_keys = [col for col in business_keys if col in df_clean.columns]

        initial_rows = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=existing_keys, keep='first')
        duplicates_removed = initial_rows - len(df_clean)

        if duplicates_removed > 0:
            logger.info(f"Duplicatas removidas: {duplicates_removed}")

        # 2. Limpeza de strings - remover espaços, padronizar case
        string_columns = df_clean.select_dtypes(include=['object']).columns
        for col in string_columns:
            if col not in ['bronze_load_timestamp', 'data_coleta_parsed']:
                df_clean[col] = df_clean[col].astype(str).str.strip().str.upper()

        # 3. Validar e limpar valores numéricos
        numeric_columns = ['Valor_Venda', 'Valor_Compra']
        for col in numeric_columns:
            if col in df_clean.columns:
                # Converter para numérico, colocando NaN em valores inválidos
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

                # Remover valores negativos ou zero
                df_clean = df_clean[df_clean[col] > 0]

                # Remover outliers extremos (acima de 3 desvios padrão)
                mean_val = df_clean[col].mean()
                std_val = df_clean[col].std()
                outlier_threshold = mean_val + 3 * std_val

                initial_count = len(df_clean)
                df_clean = df_clean[df_clean[col] <= outlier_threshold]
                outliers_removed = initial_count - len(df_clean)

                if outliers_removed > 0:
                    logger.info(f"Outliers removidos em {col}: {outliers_removed}")

        # 4. Validar datas
        if 'Data_Coleta' in df_clean.columns:
            # Verificar formato de data
            df_clean['data_coleta_parsed'] = pd.to_datetime(df_clean['Data_Coleta'], format='%d/%m/%Y', errors='coerce')

            # Remover registros com datas inválidas
            invalid_dates = df_clean['data_coleta_parsed'].isna().sum()
            if invalid_dates > 0:
                logger.warning(f"Registros com datas inválidas removidos: {invalid_dates}")
                df_clean = df_clean.dropna(subset=['data_coleta_parsed'])

        logger.info(f"Limpeza concluída: {len(df)} → {len(df_clean)} registros")
        return df_clean

    def normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza dados seguindo padrões de negócio

        Args:
            df: DataFrame para normalizar

        Returns:
            DataFrame normalizado
        """
        logger.info("Aplicando normalização de dados")

        df_norm = df.copy()

        # 1. Normalizar produtos
        product_mapping = {
            'GASOLINA': 'GASOLINA COMUM',
            'GASOLINA COMUM': 'GASOLINA COMUM',
            'GASOLINA ADITIVADA': 'GASOLINA ADITIVADA',
            'ALCOOL': 'ETANOL',
            'ETANOL': 'ETANOL',
            'DIESEL': 'ÓLEO DIESEL',
            'ÓLEO DIESEL': 'ÓLEO DIESEL',
            'ÓLEO DIESEL S10': 'ÓLEO DIESEL S10',
            'DIESEL S10': 'ÓLEO DIESEL S10',
            'GNV': 'GNV',
            'GLP': 'GLP'
        }

        df_norm['produto_normalizado'] = df_norm['Produto'].map(product_mapping).fillna(df_norm['Produto'])

        # 2. Normalizar regiões
        region_mapping = {
            'N': 'NORTE',
            'NORTE': 'NORTE',
            'NE': 'NORDESTE', 
            'NORDESTE': 'NORDESTE',
            'CO': 'CENTRO-OESTE',
            'CENTRO-OESTE': 'CENTRO-OESTE',
            'CENTRO OESTE': 'CENTRO-OESTE',
            'SE': 'SUDESTE',
            'SUDESTE': 'SUDESTE',
            'S': 'SUL',
            'SUL': 'SUL'
        }

        df_norm['regiao_normalizada'] = df_norm['Regiao'].map(region_mapping).fillna(df_norm['Regiao'])

        # 3. Normalizar bandeiras
        df_norm['bandeira_normalizada'] = df_norm['Bandeira'].str.replace('POSTO', '').str.strip()
        df_norm.loc[df_norm['bandeira_normalizada'].isin(['', 'DA ESQUINA', 'CONVENIENCIA']), 'bandeira_normalizada'] = 'BRANCA'

        # 4. Normalizar CNPJ - remover formatação
        if 'CNPJ' in df_norm.columns:
            df_norm['cnpj_limpo'] = df_norm['CNPJ'].str.replace(r'[^0-9]', '', regex=True)

        logger.info("Normalização concluída")
        return df_norm

    def enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enriquece dados com informações derivadas e métricas de negócio

        Args:
            df: DataFrame para enriquecer

        Returns:
            DataFrame enriquecido
        """
        logger.info("Aplicando enriquecimento de dados")

        df_enriched = df.copy()

        # 1. Dimensões temporais
        if 'data_coleta_parsed' in df_enriched.columns:
            df_enriched['ano_coleta'] = df_enriched['data_coleta_parsed'].dt.year
            df_enriched['mes_coleta'] = df_enriched['data_coleta_parsed'].dt.month
            df_enriched['trimestre'] = df_enriched['data_coleta_parsed'].dt.quarter
            df_enriched['semestre'] = df_enriched['data_coleta_parsed'].dt.month.apply(lambda x: 1 if x <= 6 else 2)
            df_enriched['dia_semana'] = df_enriched['data_coleta_parsed'].dt.dayofweek
            df_enriched['nome_mes'] = df_enriched['data_coleta_parsed'].dt.strftime('%B')

        # 2. Métricas de negócio
        if 'Valor_Venda' in df_enriched.columns and 'Valor_Compra' in df_enriched.columns:
            # Margem absoluta e percentual
            df_enriched['margem_absoluta'] = df_enriched['Valor_Venda'] - df_enriched['Valor_Compra']
            df_enriched['margem_percentual'] = (df_enriched['margem_absoluta'] / df_enriched['Valor_Compra']) * 100

        # 3. Categorização de produtos
        def categorize_product(product):
            if 'GASOLINA' in product:
                return 'COMBUSTIVEL_LIQUIDO'
            elif 'ETANOL' in product or 'ALCOOL' in product:
                return 'COMBUSTIVEL_RENOVAVEL'
            elif 'DIESEL' in product:
                return 'COMBUSTIVEL_DIESEL'
            elif 'GNV' in product:
                return 'COMBUSTIVEL_GASOSO'
            elif 'GLP' in product:
                return 'GAS_COZINHA'
            else:
                return 'OUTROS'

        df_enriched['categoria_produto'] = df_enriched['produto_normalizado'].apply(categorize_product)

        # 4. Classificação de bandeira
        bandeiras_grandes = ['PETROBRAS', 'SHELL', 'IPIRANGA', 'RAIZEN', 'ALESAT']
        df_enriched['tipo_bandeira'] = df_enriched['bandeira_normalizada'].apply(
            lambda x: 'GRANDE' if x in bandeiras_grandes else 'REGIONAL'
        )

        # 5. Análise de viabilidade do etanol
        if 'produto_normalizado' in df_enriched.columns and 'Valor_Venda' in df_enriched.columns:
            # Calcular viabilidade do etanol por estado/data
            viabilidade_etanol = []

            for _, row in df_enriched.iterrows():
                if row['produto_normalizado'] == 'ETANOL':
                    # Buscar preço da gasolina no mesmo estado/data
                    mask_gasolina = (
                        (df_enriched['Estado'] == row['Estado']) &
                        (df_enriched['data_coleta_parsed'] == row['data_coleta_parsed']) &
                        (df_enriched['produto_normalizado'] == 'GASOLINA COMUM')
                    )

                    precos_gasolina = df_enriched.loc[mask_gasolina, 'Valor_Venda']

                    if not precos_gasolina.empty:
                        preco_gasolina_medio = precos_gasolina.mean()
                        ratio = row['Valor_Venda'] / preco_gasolina_medio
                        viavel = ratio <= self.analytics_config.viabilidade_etanol_threshold
                        viabilidade_etanol.append(viavel)
                    else:
                        viabilidade_etanol.append(None)
                else:
                    viabilidade_etanol.append(None)

            df_enriched['etanol_viavel'] = viabilidade_etanol

        # 6. Faixas de preço
        if 'Valor_Venda' in df_enriched.columns:
            df_enriched['faixa_preco'] = pd.cut(
                df_enriched['Valor_Venda'],
                bins=[0, 2, 4, 6, 8, float('inf')],
                labels=['MUITO_BAIXO', 'BAIXO', 'MEDIO', 'ALTO', 'MUITO_ALTO'],
                include_lowest=True
            )

        logger.info("Enriquecimento concluído")
        return df_enriched

    def validate_silver_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida dados da camada Silver

        Args:
            df: DataFrame para validar

        Returns:
            Relatório de qualidade
        """
        logger.info("Validando dados da camada Silver")

        # Regras de validação para Silver
        validation_rules = {
            'produto_normalizado': {
                'valid_values': self.quality_config.valid_produtos
            },
            'regiao_normalizada': {
                'valid_values': self.quality_config.valid_regioes
            },
            'Valor_Venda': {
                'min_value': self.quality_config.min_preco,
                'max_value': self.quality_config.max_preco
            },
            'margem_percentual': {
                'min_value': 0,
                'max_value': 100
            }
        }

        # Gerar relatório
        quality_report = self.quality_checker.generate_quality_report(
            df,
            validation_rules=validation_rules,
            key_columns=['cnpj_limpo', 'data_coleta_parsed', 'produto_normalizado']
        )

        return quality_report

    def save_silver_data(self, df: pd.DataFrame) -> bool:
        """
        Salva dados na camada Silver

        Args:
            df: DataFrame para salvar

        Returns:
            True se sucesso, False caso contrário
        """
        try:
            logger.info("Salvando dados na camada Silver")

            # Salvar dados por ano/mês para otimizar consultas
            for (ano, mes), group_df in df.groupby(['ano_coleta', 'mes_coleta']):
                partition_path = self.silver_path / f"ano={ano}" / f"mes={mes}"
                partition_path.mkdir(parents=True, exist_ok=True)

                file_name = f"combustiveis_silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                file_path = partition_path / file_name

                # Remover colunas de partição antes de salvar
                df_to_save = group_df.drop(columns=['ano_coleta', 'mes_coleta'])

                df_to_save.to_parquet(
                    file_path,
                    compression='snappy',
                    index=False,
                    engine='pyarrow'
                )

                logger.info(f"Partição Silver salva: {file_path} ({len(df_to_save)} registros)")

            return True

        except Exception as e:
            logger.error(f"Erro ao salvar dados Silver: {str(e)}")
            return False

    def execute(self) -> Dict[str, Any]:
        """
        Executa o job completo de transformação Silver

        Returns:
            Relatório de execução
        """
        execution_start = datetime.now()
        logger.info("=== INICIANDO JOB SILVER TRANSFORMATION ===")

        try:
            # 1. Ler dados da Bronze
            df_bronze = self.read_bronze_data()

            # 2. Aplicar limpeza
            df_clean = self.clean_data(df_bronze)

            # 3. Normalizar dados
            df_normalized = self.normalize_data(df_clean)

            # 4. Enriquecer dados
            df_enriched = self.enrich_data(df_normalized)

            # 5. Validar qualidade
            quality_report = self.validate_silver_data(df_enriched)

            # 6. Salvar na Silver
            save_success = self.save_silver_data(df_enriched)

            if not save_success:
                raise Exception("Falha ao salvar dados na camada Silver")

            # 7. Preparar relatório
            execution_end = datetime.now()
            execution_time = (execution_end - execution_start).total_seconds()

            execution_report = {
                "job_name": "silver_transformation",
                "execution_timestamp": execution_start.isoformat(),
                "execution_time_seconds": execution_time,
                "input_records": len(df_bronze),
                "output_records": len(df_enriched),
                "quality_score": quality_report["overall_quality_score"],
                "partitions_created": len(df_enriched.groupby(['ano_coleta', 'mes_coleta'])),
                "silver_path": str(self.silver_path),
                "status": "success"
            }

            logger.info(f"=== JOB SILVER CONCLUÍDO COM SUCESSO ({execution_time:.1f}s) ===")
            logger.info(f"Registros processados: {len(df_bronze)} → {len(df_enriched)}")
            logger.info(f"Qualidade dos dados: {quality_report['overall_quality_score']:.2%}")

            return execution_report

        except Exception as e:
            execution_end = datetime.now()
            execution_time = (execution_end - execution_start).total_seconds()

            error_report = {
                "job_name": "silver_transformation",
                "execution_timestamp": execution_start.isoformat(),
                "execution_time_seconds": execution_time,
                "status": "error",
                "error_message": str(e)
            }

            logger.error(f"=== JOB SILVER FALHOU ({execution_time:.1f}s) ===")
            logger.error(f"Erro: {str(e)}")

            return error_report

if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )

    # Executar job
    job = SilverTransformationJob()
    result = job.execute()

    print("\n" + "="*50)
    print("RELATÓRIO DE EXECUÇÃO - SILVER TRANSFORMATION")
    print("="*50)
    for key, value in result.items():
        print(f"{key}: {value}")
