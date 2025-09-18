# -*- coding: utf-8 -*-
"""
Job de Ingestão da Camada Bronze
Responsável por ingerir dados brutos de combustíveis e salvar em formato Parquet
Implementa padrões de Data Lake e qualidade de dados
"""

import sys
import os
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.config import datalake_config, source_config, quality_config
from utils.data_quality.quality_checker import DataQualityChecker
from utils.file_handlers.download_utils import FuelDataDownloader, FileHandler

logger = logging.getLogger(__name__)

class BronzeIngestionJob:
    """
    Job de ingestão para camada Bronze
    Processa dados brutos e gera arquivos Parquet particionados
    """

    def __init__(self):
        """Inicializa o job de ingestão Bronze"""
        self.config = datalake_config
        self.source_config = source_config
        self.quality_config = quality_config
        self.downloader = FuelDataDownloader()
        self.quality_checker = DataQualityChecker(quality_config)
        self.file_handler = FileHandler()

        # Configurar paths
        self.bronze_path = self.config.bronze_path / "combustiveis"
        self.transient_path = self.config.transient_path / "combustiveis_raw"

        # Garantir que diretórios existem
        self.bronze_path.mkdir(parents=True, exist_ok=True)
        self.transient_path.mkdir(parents=True, exist_ok=True)

    def generate_and_save_sample_data(self, num_records: int = 50000) -> Path:
        """
        Gera e salva dados sintéticos para demonstração

        Args:
            num_records: Número de registros a gerar

        Returns:
            Path do arquivo CSV gerado
        """
        logger.info(f"Gerando dados sintéticos para demonstração ({num_records} registros)")

        # Gerar dados sintéticos
        df_sample = self.downloader.generate_sample_fuel_data(num_records)

        # Salvar em área transiente
        csv_path = self.transient_path / f"combustiveis_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        if self.downloader.save_as_csv(df_sample, csv_path):
            logger.info(f"Dados sintéticos salvos: {csv_path}")
            return csv_path
        else:
            raise Exception(f"Falha ao salvar dados sintéticos em {csv_path}")

    def validate_raw_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida dados brutos usando regras de qualidade

        Args:
            df: DataFrame com dados brutos

        Returns:
            Relatório de qualidade
        """
        logger.info("Executando validação de dados brutos")

        # Validar schema
        schema_valid = self.quality_checker.validate_fuel_data_schema(
            df, self.source_config.expected_columns
        )

        if not schema_valid:
            raise ValueError("Schema de dados inválido - colunas obrigatórias ausentes")

        # Definir regras de validação específicas
        validation_rules = {
            "Valor_Venda": {
                "min_value": self.quality_config.min_preco,
                "max_value": self.quality_config.max_preco
            },
            "Valor_Compra": {
                "min_value": self.quality_config.min_preco,
                "max_value": self.quality_config.max_preco
            },
            "Produto": {
                "valid_values": self.quality_config.valid_produtos
            },
            "Regiao": {
                "valid_values": self.quality_config.valid_regioes
            },
            "Data_Coleta": {
                "date_format": "%d/%m/%Y"
            }
        }

        # Gerar relatório de qualidade
        quality_report = self.quality_checker.generate_quality_report(
            df, 
            validation_rules=validation_rules,
            key_columns=["CNPJ", "Data_Coleta", "Produto"]
        )

        # Verificar se qualidade atende critérios mínimos
        if quality_report["overall_quality_score"] < self.quality_config.min_quality_score:
            issues = self.quality_checker.get_quality_issues(quality_report)
            logger.warning(f"Qualidade abaixo do esperado: {issues}")

        return quality_report

    def add_technical_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona colunas técnicas para controle do pipeline

        Args:
            df: DataFrame original

        Returns:
            DataFrame com colunas técnicas adicionadas
        """
        logger.info("Adicionando colunas técnicas de controle")

        df_bronze = df.copy()

        # Colunas de controle técnico
        df_bronze["bronze_load_timestamp"] = datetime.now()
        df_bronze["bronze_source_file"] = "sample_data_generation"
        df_bronze["bronze_record_id"] = range(1, len(df_bronze) + 1)

        # Parse da data de coleta para criar partições
        df_bronze["data_coleta_parsed"] = pd.to_datetime(df_bronze["Data_Coleta"], format="%d/%m/%Y")
        df_bronze["ano"] = df_bronze["data_coleta_parsed"].dt.year
        df_bronze["mes"] = df_bronze["data_coleta_parsed"].dt.month

        logger.info(f"Colunas técnicas adicionadas. Dados particionados por ano/mês: {df_bronze['ano'].nunique()} anos, {df_bronze.groupby('ano')['mes'].nunique().sum()} partições ano/mês")

        return df_bronze

    def save_to_parquet(self, df: pd.DataFrame, partition_cols: List[str] = None) -> bool:
        """
        Salva dados em formato Parquet com particionamento

        Args:
            df: DataFrame para salvar
            partition_cols: Colunas para particionamento

        Returns:
            True se sucesso, False caso contrário
        """
        if partition_cols is None:
            partition_cols = self.config.partition_columns

        try:
            logger.info(f"Salvando dados em Parquet com particionamento por: {partition_cols}")

            # Verificar se colunas de partição existem
            missing_cols = set(partition_cols) - set(df.columns)
            if missing_cols:
                logger.error(f"Colunas de partição ausentes: {missing_cols}")
                return False

            # Salvar por partições
            for partition_values, group_df in df.groupby(partition_cols):
                # Criar path da partição
                if isinstance(partition_values, tuple):
                    partition_path = self.bronze_path
                    for i, col in enumerate(partition_cols):
                        partition_path = partition_path / f"{col}={partition_values[i]}"
                else:
                    partition_path = self.bronze_path / f"{partition_cols[0]}={partition_values}"

                partition_path.mkdir(parents=True, exist_ok=True)

                # Nome do arquivo com timestamp
                file_name = f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                file_path = partition_path / file_name

                # Remover colunas de partição do DataFrame (não são necessárias no arquivo)
                df_to_save = group_df.drop(columns=partition_cols)

                # Salvar em Parquet com compressão
                df_to_save.to_parquet(
                    file_path,
                    compression="snappy",
                    index=False,
                    engine="pyarrow"
                )

                logger.info(f"Partição salva: {file_path} ({len(df_to_save)} registros)")

            logger.info(f"Dados salvos em Bronze com sucesso: {len(df)} registros em {len(df.groupby(partition_cols))} partições")
            return True

        except Exception as e:
            logger.error(f"Erro ao salvar em Parquet: {str(e)}")
            return False

    def execute(self, input_file: Optional[Path] = None, num_sample_records: int = 50000) -> Dict[str, Any]:
        """
        Executa o job completo de ingestão Bronze

        Args:
            input_file: Arquivo CSV de entrada (None para gerar dados sintéticos)
            num_sample_records: Número de registros sintéticos a gerar

        Returns:
            Relatório de execução
        """
        execution_start = datetime.now()
        logger.info("=== INICIANDO JOB BRONZE INGESTION ===")

        try:
            # 1. Obter dados de entrada
            if input_file and input_file.exists():
                logger.info(f"Carregando dados de: {input_file}")
                df_raw = pd.read_csv(input_file, encoding="utf-8")
            else:
                logger.info("Gerando dados sintéticos para demonstração")
                csv_path = self.generate_and_save_sample_data(num_sample_records)
                df_raw = pd.read_csv(csv_path, encoding="utf-8")

            logger.info(f"Dados carregados: {len(df_raw)} registros, {len(df_raw.columns)} colunas")

            # 2. Validar qualidade dos dados
            quality_report = self.validate_raw_data(df_raw)

            # 3. Adicionar colunas técnicas
            df_bronze = self.add_technical_columns(df_raw)

            # 4. Salvar em formato Parquet
            save_success = self.save_to_parquet(df_bronze)

            if not save_success:
                raise Exception("Falha ao salvar dados em formato Parquet")

            # 5. Preparar relatório de execução
            execution_end = datetime.now()
            execution_time = (execution_end - execution_start).total_seconds()

            execution_report = {
                "job_name": "bronze_ingestion",
                "execution_timestamp": execution_start.isoformat(),
                "execution_time_seconds": execution_time,
                "input_records": len(df_raw),
                "output_records": len(df_bronze),
                "partitions_created": len(df_bronze.groupby(self.config.partition_columns)),
                "quality_score": quality_report["overall_quality_score"],
                "bronze_path": str(self.bronze_path),
                "status": "success"
            }

            logger.info(f"=== JOB BRONZE CONCLUÍDO COM SUCESSO ({execution_time:.1f}s) ===")
            logger.info(f"Registros processados: {len(df_raw)} → {len(df_bronze)}")
            logger.info(f"Qualidade dos dados: {quality_report['overall_quality_score']:.2%}")
            logger.info(f"Partições criadas: {execution_report['partitions_created']}")

            return execution_report

        except Exception as e:
            execution_end = datetime.now()
            execution_time = (execution_end - execution_start).total_seconds()

            error_report = {
                "job_name": "bronze_ingestion",
                "execution_timestamp": execution_start.isoformat(),
                "execution_time_seconds": execution_time,
                "status": "error",
                "error_message": str(e)
            }

            logger.error(f"=== JOB BRONZE FALHOU ({execution_time:.1f}s) ===")
            logger.error(f"Erro: {str(e)}")

            return error_report

    def get_bronze_data_info(self) -> Dict[str, Any]:
        """
        Obtém informações sobre dados na camada Bronze

        Returns:
            Informações sobre partições e arquivos
        """
        info = {
            "bronze_path": str(self.bronze_path),
            "partitions": [],
            "total_files": 0,
            "total_size_mb": 0.0
        }

        if not self.bronze_path.exists():
            return info

        # Percorrer partições
        for partition_dir in self.bronze_path.rglob("*/"):
            if partition_dir.is_dir():
                parquet_files = list(partition_dir.glob("*.parquet"))
                if parquet_files:
                    partition_size = sum(f.stat().st_size for f in parquet_files)
                    partition_info = {
                        "path": str(partition_dir.relative_to(self.bronze_path)),
                        "files": len(parquet_files),
                        "size_mb": partition_size / 1024 / 1024
                    }
                    info["partitions"].append(partition_info)
                    info["total_files"] += len(parquet_files)
                    info["total_size_mb"] += partition_info["size_mb"]

        return info

if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )

    # Executar job
    job = BronzeIngestionJob()
    result = job.execute()

    print("\n" + "="*50)
    print("RELATÓRIO DE EXECUÇÃO - BRONZE INGESTION")
    print("="*50)
    for key, value in result.items():
        print(f"{key}: {value}")
