# -*- coding: utf-8 -*-
"""
Módulo de verificação de qualidade de dados
Implementa métricas e validações para o pipeline SGA
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """
    Classe para verificação de qualidade de dados
    Implementa métricas padrão da indústria
    """

    def __init__(self, config=None):
        """
        Inicializa o verificador de qualidade

        Args:
            config: Configuração de qualidade (QualityConfig)
        """
        self.config = config

    def calculate_completeness(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Calcula completude por coluna

        Args:
            df: DataFrame para análise

        Returns:
            Dicionário com percentual de completude por coluna
        """
        completeness = {}
        total_rows = len(df)

        if total_rows == 0:
            return completeness

        for column in df.columns:
            non_null_count = df[column].count()
            completeness[column] = non_null_count / total_rows

        return completeness

    def calculate_validity(self, df: pd.DataFrame, validation_rules: Dict[str, Any]) -> Dict[str, float]:
        """
        Calcula validade baseada em regras de negócio

        Args:
            df: DataFrame para análise
            validation_rules: Regras de validação por coluna

        Returns:
            Dicionário com percentual de validade por coluna
        """
        validity = {}
        total_rows = len(df)

        if total_rows == 0:
            return validity

        for column, rules in validation_rules.items():
            if column not in df.columns:
                validity[column] = 0.0
                continue

            valid_count = total_rows

            # Validação de range numérico
            if 'min_value' in rules and 'max_value' in rules:
                valid_mask = (
                    (df[column] >= rules['min_value']) & 
                    (df[column] <= rules['max_value'])
                )
                valid_count = valid_mask.sum()

            # Validação de valores permitidos
            elif 'valid_values' in rules:
                valid_mask = df[column].isin(rules['valid_values'])
                valid_count = valid_mask.sum()

            # Validação de formato de data
            elif 'date_format' in rules:
                try:
                    pd.to_datetime(df[column], format=rules['date_format'], errors='coerce')
                    valid_mask = pd.to_datetime(df[column], errors='coerce').notna()
                    valid_count = valid_mask.sum()
                except:
                    valid_count = 0

            validity[column] = valid_count / total_rows if total_rows > 0 else 0.0

        return validity

    def calculate_uniqueness(self, df: pd.DataFrame, key_columns: List[str]) -> float:
        """
        Calcula unicidade baseada em chaves de negócio

        Args:
            df: DataFrame para análise
            key_columns: Colunas que formam chave de negócio

        Returns:
            Percentual de unicidade
        """
        if not key_columns or len(df) == 0:
            return 1.0

        # Verificar se colunas existem
        existing_columns = [col for col in key_columns if col in df.columns]
        if not existing_columns:
            return 0.0

        total_rows = len(df)
        unique_rows = df[existing_columns].drop_duplicates().shape[0]

        return unique_rows / total_rows

    def calculate_consistency(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Calcula consistência de formatos e padrões

        Args:
            df: DataFrame para análise

        Returns:
            Dicionário com métricas de consistência
        """
        consistency = {}

        for column in df.columns:
            if df[column].dtype == 'object':
                # Verificar consistência de formato para strings
                non_null_series = df[column].dropna()
                if len(non_null_series) == 0:
                    consistency[column] = 1.0
                    continue

                # Calcular consistência baseada em padrões
                patterns = non_null_series.astype(str).str.len().value_counts()
                max_pattern_count = patterns.max() if len(patterns) > 0 else 0
                consistency[column] = max_pattern_count / len(non_null_series)
            else:
                # Para colunas numéricas, verificar consistência de tipo
                consistency[column] = 1.0 - (df[column].isna().sum() / len(df))

        return consistency

    def generate_quality_report(self, df: pd.DataFrame, 
                              validation_rules: Dict[str, Any] = None,
                              key_columns: List[str] = None) -> Dict[str, Any]:
        """
        Gera relatório completo de qualidade

        Args:
            df: DataFrame para análise
            validation_rules: Regras de validação
            key_columns: Colunas chave para unicidade

        Returns:
            Relatório completo de qualidade
        """
        logger.info(f"Gerando relatório de qualidade para dataset com {len(df)} registros")

        report = {
            'timestamp': datetime.now().isoformat(),
            'dataset_info': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
            },
            'completeness': self.calculate_completeness(df),
            'consistency': self.calculate_consistency(df)
        }

        # Adicionar métricas de validade se regras fornecidas
        if validation_rules:
            report['validity'] = self.calculate_validity(df, validation_rules)

        # Adicionar métrica de unicidade se chaves fornecidas  
        if key_columns:
            report['uniqueness'] = self.calculate_uniqueness(df, key_columns)

        # Calcular score geral de qualidade
        report['overall_quality_score'] = self._calculate_overall_score(report)

        logger.info(f"Score de qualidade geral: {report['overall_quality_score']:.2%}")

        return report

    def _calculate_overall_score(self, report: Dict[str, Any]) -> float:
        """
        Calcula score geral de qualidade

        Args:
            report: Relatório de qualidade

        Returns:
            Score geral (0.0 a 1.0)
        """
        scores = []

        # Score de completude (média das colunas)
        if 'completeness' in report:
            completeness_scores = list(report['completeness'].values())
            if completeness_scores:
                scores.append(np.mean(completeness_scores))

        # Score de validade (média das colunas)
        if 'validity' in report:
            validity_scores = list(report['validity'].values())
            if validity_scores:
                scores.append(np.mean(validity_scores))

        # Score de consistência (média das colunas)
        if 'consistency' in report:
            consistency_scores = list(report['consistency'].values())
            if consistency_scores:
                scores.append(np.mean(consistency_scores))

        # Score de unicidade
        if 'uniqueness' in report:
            scores.append(report['uniqueness'])

        return np.mean(scores) if scores else 0.0

    def validate_fuel_data_schema(self, df: pd.DataFrame, expected_columns: List[str]) -> bool:
        """
        Valida schema específico para dados de combustíveis

        Args:
            df: DataFrame com dados de combustíveis
            expected_columns: Colunas esperadas

        Returns:
            True se schema válido, False caso contrário
        """
        missing_columns = set(expected_columns) - set(df.columns)

        if missing_columns:
            logger.error(f"Colunas obrigatórias ausentes: {missing_columns}")
            return False

        logger.info("Schema de dados de combustíveis validado com sucesso")
        return True

    def get_quality_issues(self, report: Dict[str, Any]) -> List[str]:
        """
        Identifica problemas de qualidade baseado no relatório

        Args:
            report: Relatório de qualidade

        Returns:
            Lista de problemas identificados
        """
        issues = []

        # Verificar completude
        if 'completeness' in report:
            for column, score in report['completeness'].items():
                if score < 0.9:  # Menos de 90% completo
                    issues.append(f"Coluna '{column}' com baixa completude: {score:.1%}")

        # Verificar validade
        if 'validity' in report:
            for column, score in report['validity'].items():
                if score < 0.8:  # Menos de 80% válido
                    issues.append(f"Coluna '{column}' com baixa validade: {score:.1%}")

        # Verificar unicidade
        if 'uniqueness' in report and report['uniqueness'] < 0.95:
            issues.append(f"Baixa unicidade detectada: {report['uniqueness']:.1%}")

        # Verificar score geral
        if report['overall_quality_score'] < 0.8:
            issues.append(f"Score geral de qualidade abaixo do esperado: {report['overall_quality_score']:.1%}")

        return issues
