# -*- coding: utf-8 -*-
"""
Utilitários para download e manipulação de arquivos
Preparado para lidar com dados do dados.gov.br
"""

import os
import requests
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime
import time

logger = logging.getLogger(__name__)

class FuelDataDownloader:
    """
    Classe para download de dados de combustíveis
    Simulação para dados do dados.gov.br
    """

    def __init__(self, base_url: str = None):
        """
        Inicializa o downloader

        Args:
            base_url: URL base para download dos dados
        """
        self.base_url = base_url or "https://dados.gov.br"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SGA-Data-Pipeline/1.0'
        })

    def download_file(self, url: str, destination: Path, 
                     chunk_size: int = 8192, timeout: int = 300) -> bool:
        """
        Download de arquivo com retry e progress

        Args:
            url: URL do arquivo
            destination: Caminho de destino
            chunk_size: Tamanho do chunk em bytes
            timeout: Timeout em segundos

        Returns:
            True se sucesso, False caso contrário
        """
        try:
            # Criar diretório se não existir
            destination.parent.mkdir(parents=True, exist_ok=True)

            logger.info(f"Iniciando download de {url}")

            response = self.session.get(url, stream=True, timeout=timeout)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            with open(destination, 'wb') as file:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        file.write(chunk)
                        downloaded += len(chunk)

                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            if downloaded % (chunk_size * 100) == 0:  # Log a cada ~800KB
                                logger.info(f"Download: {progress:.1f}% ({downloaded}/{total_size} bytes)")

            logger.info(f"Download concluído: {destination}")
            return True

        except Exception as e:
            logger.error(f"Erro no download de {url}: {str(e)}")
            if destination.exists():
                destination.unlink()  # Remove arquivo parcial
            return False

    def generate_sample_fuel_data(self, num_records: int = 10000) -> pd.DataFrame:
        """
        Gera dados sintéticos de combustíveis para demonstração
        Mantém estrutura e padrões realistas

        Args:
            num_records: Número de registros a gerar

        Returns:
            DataFrame com dados sintéticos
        """
        import random
        import numpy as np
        from datetime import datetime, timedelta

        logger.info(f"Gerando {num_records} registros sintéticos de combustíveis")

        # Definições base
        regioes = ["NORTE", "NORDESTE", "CENTRO-OESTE", "SUDESTE", "SUL"]
        estados_por_regiao = {
            "NORTE": ["AM", "RR", "AP", "PA", "TO", "RO", "AC"],
            "NORDESTE": ["MA", "PI", "CE", "RN", "PB", "PE", "AL", "SE", "BA"],
            "CENTRO-OESTE": ["MT", "MS", "GO", "DF"],
            "SUDESTE": ["SP", "RJ", "MG", "ES"],
            "SUL": ["PR", "SC", "RS"]
        }

        produtos = [
            "GASOLINA COMUM", "GASOLINA ADITIVADA", "ETANOL", 
            "ÓLEO DIESEL", "ÓLEO DIESEL S10", "GNV", "GLP"
        ]

        bandeiras = [
            "PETROBRAS", "SHELL", "IPIRANGA", "ALESAT", "RAIZEN",
            "BRANCA", "EQUADOR", "TEXACO", "POSTO DA ESQUINA"
        ]

        # Preços base por produto (R$)
        precos_base = {
            "GASOLINA COMUM": 5.20,
            "GASOLINA ADITIVADA": 5.50,
            "ETANOL": 3.80,
            "ÓLEO DIESEL": 4.80,
            "ÓLEO DIESEL S10": 5.00,
            "GNV": 4.20,
            "GLP": 110.0
        }

        records = []
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2024, 12, 31)

        for _ in range(num_records):
            # Selecionar região e estado
            regiao = random.choice(regioes)
            estado = random.choice(estados_por_regiao[regiao])

            # Gerar dados básicos
            municipio = f"CIDADE-{random.randint(1000, 9999)}"
            revenda = f"POSTO {random.choice(['CENTRAL', 'SUL', 'NORTE', 'CENTRO'])}"
            cnpj = f"{random.randint(10, 99)}.{random.randint(100, 999)}.{random.randint(100, 999)}/0001-{random.randint(10, 99)}"
            endereco = f"RUA {random.choice(['A', 'B', 'C', 'PRINCIPAL'])}, {random.randint(1, 999)}"

            produto = random.choice(produtos)
            bandeira = random.choice(bandeiras)

            # Data aleatória no período
            random_date = start_date + timedelta(
                days=random.randint(0, (end_date - start_date).days)
            )
            data_coleta = random_date.strftime("%d/%m/%Y")

            # Preços com variação realística
            preco_base = precos_base[produto]
            variacao = random.uniform(0.8, 1.2)  # ±20% de variação
            valor_venda = round(preco_base * variacao, 3)
            valor_compra = round(valor_venda * random.uniform(0.85, 0.95), 3)  # 5-15% de margem

            unidade = "R$ / litro" if produto != "GLP" else "R$ / 13Kg"

            records.append({
                "Regiao": regiao,
                "Estado": estado,
                "Municipio": municipio,
                "Revenda": revenda,
                "CNPJ": cnpj,
                "Endereco": endereco,
                "Produto": produto,
                "Data_Coleta": data_coleta,
                "Valor_Venda": valor_venda,
                "Valor_Compra": valor_compra,
                "Unidade_Medida": unidade,
                "Bandeira": bandeira
            })

        df = pd.DataFrame(records)
        logger.info(f"Dados sintéticos gerados: {len(df)} registros, {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f}MB")

        return df

    def save_as_csv(self, df: pd.DataFrame, file_path: Path, 
                   encoding: str = 'utf-8', sep: str = ',') -> bool:
        """
        Salva DataFrame como CSV

        Args:
            df: DataFrame para salvar
            file_path: Caminho do arquivo
            encoding: Codificação do arquivo
            sep: Separador CSV

        Returns:
            True se sucesso, False caso contrário
        """
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(file_path, encoding=encoding, sep=sep, index=False)
            logger.info(f"CSV salvo: {file_path} ({len(df)} registros)")
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar CSV {file_path}: {str(e)}")
            return False

class FileHandler:
    """
    Classe para manipulação geral de arquivos
    """

    @staticmethod
    def ensure_directory(path: Path) -> None:
        """
        Garante que diretório existe

        Args:
            path: Caminho do diretório
        """
        path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def get_file_info(file_path: Path) -> Dict[str, Any]:
        """
        Obtém informações sobre arquivo

        Args:
            file_path: Caminho do arquivo

        Returns:
            Dicionário com informações do arquivo
        """
        if not file_path.exists():
            return {"exists": False}

        stat = file_path.stat()
        return {
            "exists": True,
            "size_bytes": stat.st_size,
            "size_mb": stat.st_size / 1024 / 1024,
            "created": datetime.fromtimestamp(stat.st_ctime),
            "modified": datetime.fromtimestamp(stat.st_mtime),
            "extension": file_path.suffix.lower()
        }

    @staticmethod
    def clean_old_files(directory: Path, max_age_days: int = 7) -> int:
        """
        Remove arquivos antigos de um diretório

        Args:
            directory: Diretório para limpar
            max_age_days: Idade máxima em dias

        Returns:
            Número de arquivos removidos
        """
        if not directory.exists():
            return 0

        removed_count = 0
        cutoff_time = time.time() - (max_age_days * 24 * 60 * 60)

        try:
            for file_path in directory.iterdir():
                if file_path.is_file() and file_path.stat().st_mtime < cutoff_time:
                    file_path.unlink()
                    removed_count += 1
                    logger.debug(f"Arquivo removido: {file_path}")
        except Exception as e:
            logger.error(f"Erro ao limpar diretório {directory}: {str(e)}")

        if removed_count > 0:
            logger.info(f"Limpeza concluída: {removed_count} arquivos removidos de {directory}")

        return removed_count

    @staticmethod
    def validate_csv_structure(file_path: Path, expected_columns: List[str] = None) -> bool:
        """
        Valida estrutura de arquivo CSV

        Args:
            file_path: Caminho do arquivo CSV
            expected_columns: Colunas esperadas

        Returns:
            True se válido, False caso contrário
        """
        try:
            # Ler apenas header
            df_sample = pd.read_csv(file_path, nrows=0)

            if expected_columns:
                missing_columns = set(expected_columns) - set(df_sample.columns)
                if missing_columns:
                    logger.error(f"Colunas ausentes em {file_path}: {missing_columns}")
                    return False

            logger.info(f"Estrutura CSV validada: {file_path}")
            return True

        except Exception as e:
            logger.error(f"Erro ao validar CSV {file_path}: {str(e)}")
            return False
