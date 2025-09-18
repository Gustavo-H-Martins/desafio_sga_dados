# -*- coding: utf-8 -*-
"""
Configurações centralizadas para o pipeline de dados SGA
Seguindo padrões de engenharia de dados com arquitetura medalhão
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

# Diretório base do projeto
BASE_DIR = Path(__file__).parent.parent
DATALAKE_PATH = BASE_DIR / "datalake"

@dataclass
class DataLakeConfig:
    """Configurações das camadas do Data Lake"""

    # Caminhos das camadas
    transient_path: Path = DATALAKE_PATH / "camada_0_transient"
    bronze_path: Path = DATALAKE_PATH / "camada_1_bronze"
    silver_path: Path = DATALAKE_PATH / "camada_2_silver"  
    gold_path: Path = DATALAKE_PATH / "camada_3_gold"

    # Particionamento
    partition_columns: List[str] = None

    def __post_init__(self):
        """Configurações pós-inicialização"""
        if self.partition_columns is None:
            self.partition_columns = ["ano", "mes"]

        # Criar diretórios se não existirem
        for path in [self.transient_path, self.bronze_path, self.silver_path, self.gold_path]:
            path.mkdir(parents=True, exist_ok=True)

@dataclass 
class SourceDataConfig:
    """Configurações dos dados de origem"""

    # URLs e fontes de dados
    dados_gov_url: str = "https://dados.gov.br/dataset/serie-historica-de-precos-de-combustiveis-por-revenda"

    # Schema esperado dos dados de combustíveis
    expected_columns: List[str] = None

    # Validações de qualidade
    required_columns: List[str] = None

    def __post_init__(self):
        """Definir colunas esperadas"""
        if self.expected_columns is None:
            self.expected_columns = [
                "Regiao", "Estado", "Municipio", "Revenda", "CNPJ", 
                "Endereco", "Produto", "Data_Coleta", "Valor_Venda", 
                "Valor_Compra", "Unidade_Medida", "Bandeira"
            ]

        if self.required_columns is None:
            self.required_columns = [
                "Estado", "Produto", "Data_Coleta", "Valor_Venda"
            ]

@dataclass
class SparkConfig:
    """Configurações do Spark (preparado para escalabilidade)"""

    app_name: str = "SGA_Fuel_Analytics"
    master: str = "local[*]"

    # Configurações de memória e performance
    executor_memory: str = "2g"
    driver_memory: str = "2g"
    max_result_size: str = "1g"

    # Configurações de Parquet
    parquet_compression: str = "snappy"

    @property
    def spark_configs(self) -> Dict[str, str]:
        """Retorna configurações como dicionário"""
        return {
            "spark.executor.memory": self.executor_memory,
            "spark.driver.memory": self.driver_memory,
            "spark.driver.maxResultSize": self.max_result_size,
            "spark.sql.parquet.compression.codec": self.parquet_compression,
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }

@dataclass
class QualityConfig:
    """Configurações de qualidade de dados"""

    # Limites de qualidade
    min_quality_score: float = 0.8
    max_null_percentage: float = 0.1

    # Validações específicas para combustíveis
    valid_produtos: List[str] = None
    valid_regioes: List[str] = None

    # Ranges de valores válidos
    min_preco: float = 0.1
    max_preco: float = 15.0

    def __post_init__(self):
        """Definir valores válidos"""
        if self.valid_produtos is None:
            self.valid_produtos = [
                "GASOLINA COMUM", "GASOLINA ADITIVADA", 
                "ETANOL", "ÓLEO DIESEL", "ÓLEO DIESEL S10",
                "GNV", "GLP"
            ]

        if self.valid_regioes is None:
            self.valid_regioes = [
                "NORTE", "NORDESTE", "CENTRO-OESTE", 
                "SUDESTE", "SUL"
            ]

@dataclass
class AnalyticsConfig:
    """Configurações para análises de negócio"""

    # Período de análise
    start_year: int = 2020
    end_year: int = 2024

    # Métricas de negócio
    viabilidade_etanol_threshold: float = 0.7  # 70% do preço da gasolina

    # Agregações temporais
    temporal_granularities: List[str] = None

    def __post_init__(self):
        """Definir granularidades temporais"""
        if self.temporal_granularities is None:
            self.temporal_granularities = [
                "diario", "semanal", "mensal", "trimestral", "anual"
            ]

# Instâncias globais das configurações
datalake_config = DataLakeConfig()
source_config = SourceDataConfig()
spark_config = SparkConfig()
quality_config = QualityConfig()
analytics_config = AnalyticsConfig()

# Função para obter configuração específica
def get_config(config_type: str):
    """
    Obtém configuração específica por tipo

    Args:
        config_type: Tipo de configuração (datalake, source, spark, quality, analytics)

    Returns:
        Instância da configuração solicitada
    """
    configs = {
        "datalake": datalake_config,
        "source": source_config, 
        "spark": spark_config,
        "quality": quality_config,
        "analytics": analytics_config
    }

    return configs.get(config_type.lower())

# Configurações de logging
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] %(name)s [%(filename)s:%(lineno)d]: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'file': {
            'level': 'DEBUG',
            'formatter': 'detailed',
            'class': 'logging.FileHandler',
            'filename': BASE_DIR / 'logs' / 'pipeline.log',
            'mode': 'a',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': False
        }
    }
}
