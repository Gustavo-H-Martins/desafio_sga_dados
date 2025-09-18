"""
Pipeline Orchestrator - Execução Completa do Pipeline de Dados
Orquestra a execução sequencial das camadas Bronze, Silver e Gold
"""

import os
import sys
import time
import logging
from datetime import datetime
from typing import Dict, Any, List
import json

# Adicionar diretórios ao path para imports
project_root = "/home/user/output/desafio_sga_dados"
sys.path.append(project_root)

from config.config import datalake_config

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    """Orquestrador principal do pipeline de dados"""

    def __init__(self):
        self.config = datalake_config
        self.execution_log = []
        self.start_time = None
        self.end_time = None

        # Criar diretório de logs se não existir
        logs_dir = f"{project_root}/logs"
        os.makedirs(logs_dir, exist_ok=True)

    def run_full_pipeline(self) -> Dict[str, Any]:
        """Executa o pipeline completo: Bronze -> Silver -> Gold"""
        logger.info("🚀 INICIANDO EXECUÇÃO COMPLETA DO PIPELINE DE DADOS")
        logger.info("=" * 70)

        self.start_time = datetime.now()

        pipeline_summary = {
            "pipeline_name": "combustiveis_data_pipeline",
            "execution_id": f"exec_{self.start_time.strftime('%Y%m%d_%H%M%S')}",
            "start_time": self.start_time,
            "status": "running",
            "layers_executed": [],
            "total_duration": 0,
            "errors": [],
            "layer_results": {}
        }

        try:
            # 1. Executar camada Bronze
            logger.info("📊 ETAPA 1/3: Executando camada Bronze...")
            bronze_result = self._execute_bronze_layer()
            pipeline_summary["layer_results"]["bronze"] = bronze_result
            pipeline_summary["layers_executed"].append("bronze")

            if bronze_result["status"] != "completed":
                raise Exception(f"Falha na camada Bronze: {bronze_result.get('errors', [])}")

            logger.info("✅ Camada Bronze concluída com sucesso")

            # 2. Executar camada Silver
            logger.info("🔧 ETAPA 2/3: Executando camada Silver...")
            silver_result = self._execute_silver_layer()
            pipeline_summary["layer_results"]["silver"] = silver_result
            pipeline_summary["layers_executed"].append("silver")

            if silver_result["status"] != "completed":
                raise Exception(f"Falha na camada Silver: {silver_result.get('errors', [])}")

            logger.info("✅ Camada Silver concluída com sucesso")

            # 3. Executar camada Gold
            logger.info("🏆 ETAPA 3/3: Executando camada Gold...")
            gold_result = self._execute_gold_layer()
            pipeline_summary["layer_results"]["gold"] = gold_result
            pipeline_summary["layers_executed"].append("gold")

            if gold_result["status"] != "completed":
                raise Exception(f"Falha na camada Gold: {gold_result.get('errors', [])}")

            logger.info("✅ Camada Gold concluída com sucesso")

            # 4. Consolidar resultados
            pipeline_summary["status"] = "completed"
            logger.info("🎉 PIPELINE EXECUTADO COM SUCESSO!")

        except Exception as e:
            pipeline_summary["status"] = "failed"
            pipeline_summary["errors"].append(str(e))
            logger.error(f"❌ Falha na execução do pipeline: {e}")

        finally:
            self.end_time = datetime.now()
            pipeline_summary["end_time"] = self.end_time
            pipeline_summary["total_duration"] = (self.end_time - self.start_time).total_seconds()

            # Salvar log de execução
            self._save_execution_log(pipeline_summary)

            # Imprimir resumo
            self._print_execution_summary(pipeline_summary)

        return pipeline_summary

    def _execute_bronze_layer(self) -> Dict[str, Any]:
        """Executa o job da camada Bronze"""
        try:
            from jobs.bronze_layer.bronze_ingestion import BronzeIngestionJob

            bronze_job = BronzeIngestionJob()
            result = bronze_job.execute()

            logger.info(f"Bronze - Status: {result['status']}")
            logger.info(f"Bronze - Registros processados: {result.get('total_records', 0):,}")

            return result

        except Exception as e:
            logger.error(f"Erro na execução da camada Bronze: {e}")
            return {"status": "failed", "errors": [str(e)]}

    def _execute_silver_layer(self) -> Dict[str, Any]:
        """Executa o job da camada Silver"""
        try:
            from jobs.silver_layer.silver_transformation import SilverTransformationJob

            silver_job = SilverTransformationJob()
            result = silver_job.execute()

            logger.info(f"Silver - Status: {result['status']}")
            logger.info(f"Silver - Registros entrada: {result.get('records_input', 0):,}")
            logger.info(f"Silver - Registros saída: {result.get('records_output', 0):,}")

            return result

        except Exception as e:
            logger.error(f"Erro na execução da camada Silver: {e}")
            return {"status": "failed", "errors": [str(e)]}

    def _execute_gold_layer(self) -> Dict[str, Any]:
        """Executa o job da camada Gold"""
        try:
            from jobs.gold_layer.gold_analytics import GoldAnalyticsJob

            gold_job = GoldAnalyticsJob()
            result = gold_job.execute()

            logger.info(f"Gold - Status: {result['status']}")
            logger.info(f"Gold - Analytics criados: {len(result.get('analytics_created', []))}")
            logger.info(f"Gold - Datasets gerados: {len(result.get('datasets_generated', []))}")

            return result

        except Exception as e:
            logger.error(f"Erro na execução da camada Gold: {e}")
            return {"status": "failed", "errors": [str(e)]}

    def _save_execution_log(self, pipeline_summary: Dict[str, Any]):
        """Salva log de execução do pipeline"""
        log_file = f"{project_root}/logs/pipeline_execution_{pipeline_summary['execution_id']}.json"

        try:
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(pipeline_summary, f, indent=2, ensure_ascii=False, default=str)

            logger.info(f"Log de execução salvo em: {log_file}")

        except Exception as e:
            logger.error(f"Erro ao salvar log de execução: {e}")

    def _print_execution_summary(self, pipeline_summary: Dict[str, Any]):
        """Imprime resumo da execução"""

        print("\n" + "=" * 70)
        print("🎯 RESUMO DA EXECUÇÃO DO PIPELINE")
        print("=" * 70)

        print(f"📋 ID Execução: {pipeline_summary['execution_id']}")
        print(f"⏰ Início: {pipeline_summary['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🏁 Fim: {pipeline_summary['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️  Duração Total: {pipeline_summary['total_duration']:.2f} segundos")
        print(f"🎛️  Status: {pipeline_summary['status'].upper()}")

        print(f"\n🔄 Camadas Executadas: {', '.join(pipeline_summary['layers_executed'])}")

        # Resumo por camada
        for layer, result in pipeline_summary['layer_results'].items():
            status_icon = "✅" if result.get('status') == 'completed' else "❌"
            print(f"\n{status_icon} {layer.upper()}:")
            print(f"   Status: {result.get('status', 'unknown')}")
            print(f"   Duração: {result.get('duration', 0):.2f}s")

            if layer == 'bronze':
                print(f"   Arquivos processados: {result.get('files_processed', 0)}")
                print(f"   Total registros: {result.get('total_records', 0):,}")
                if result.get('quality_summary'):
                    print(f"   Score qualidade: {result['quality_summary'].get('score', 0):.1f}/100")

            elif layer == 'silver':
                print(f"   Registros entrada: {result.get('records_input', 0):,}")
                print(f"   Registros saída: {result.get('records_output', 0):,}")
                print(f"   Transformações: {len(result.get('transformations_applied', []))}")

            elif layer == 'gold':
                print(f"   Registros processados: {result.get('records_processed', 0):,}")
                print(f"   Analytics criados: {len(result.get('analytics_created', []))}")
                print(f"   Datasets gerados: {len(result.get('datasets_generated', []))}")

        # Erros
        if pipeline_summary['errors']:
            print(f"\n❌ ERROS ENCONTRADOS:")
            for error in pipeline_summary['errors']:
                print(f"   • {error}")

        # Próximos passos
        if pipeline_summary['status'] == 'completed':
            print(f"\n🎊 PRÓXIMOS PASSOS:")
            print(f"   • Executar dashboard: python dashboard/run_dashboard.py")
            print(f"   • Acessar em: http://localhost:8501")
            print(f"   • Verificar dados em: {self.config.gold_path}")

        print("=" * 70)

class DataQualityValidator:
    """Validador de qualidade entre camadas"""

    def __init__(self):
        self.config = datalake_config

    def validate_pipeline_flow(self) -> Dict[str, Any]:
        """Valida fluxo de dados entre as camadas"""
        validation_results = {
            "timestamp": datetime.now(),
            "validations": {},
            "overall_status": "unknown",
            "issues_found": []
        }

        try:
            # Validar Bronze
            bronze_validation = self._validate_bronze_layer()
            validation_results["validations"]["bronze"] = bronze_validation

            # Validar Silver (se Bronze estiver OK)
            if bronze_validation["status"] == "ok":
                silver_validation = self._validate_silver_layer()
                validation_results["validations"]["silver"] = silver_validation

                # Validar Gold (se Silver estiver OK)
                if silver_validation["status"] == "ok":
                    gold_validation = self._validate_gold_layer()
                    validation_results["validations"]["gold"] = gold_validation

            # Determinar status geral
            all_statuses = [v.get("status") for v in validation_results["validations"].values()]

            if all(status == "ok" for status in all_statuses):
                validation_results["overall_status"] = "passed"
            elif any(status == "error" for status in all_statuses):
                validation_results["overall_status"] = "failed"
            else:
                validation_results["overall_status"] = "warning"

        except Exception as e:
            validation_results["overall_status"] = "error"
            validation_results["issues_found"].append(f"Validation error: {str(e)}")

        return validation_results

    def _validate_bronze_layer(self) -> Dict[str, Any]:
        """Valida dados da camada Bronze"""
        bronze_path = f"{self.config.bronze_path}/combustiveis/ano=2020/mes=1/data_20250918_105610.parquet"

        if not os.path.exists(bronze_path):
            return {"status": "error", "message": "Arquivo Bronze não encontrado"}

        try:
            import pandas as pd
            df = pd.read_parquet(bronze_path)

            return {
                "status": "ok",
                "records": len(df),
                "columns": len(df.columns),
                "file_size_mb": round(os.path.getsize(bronze_path) / 1024 / 1024, 2)
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _validate_silver_layer(self) -> Dict[str, Any]:
        """Valida dados da camada Silver"""
        silver_path = f"{self.config.silver_path}/combustiveis_processed/ano=2020/mes=1/combustiveis_silver_20250918_105755.parquet"

        if not os.path.exists(silver_path):
            return {"status": "error", "message": "Arquivo Silver não encontrado"}

        try:
            import pandas as pd
            df = pd.read_parquet(silver_path)

            return {
                "status": "ok",
                "records": len(df),
                "columns": len(df.columns),
                "file_size_mb": round(os.path.getsize(silver_path) / 1024 / 1024, 2)
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _validate_gold_layer(self) -> Dict[str, Any]:
        """Valida dados da camada Gold"""
        gold_analytics_product_path = f"{self.config.gold_path}/analytics/product"
        gold_analytics_competitive_path = f"{self.config.gold_path}/analytics/competitive"
        gold_analytics_regional_path = f"{self.config.gold_path}/analytics/regional"
        gold_analytics_temporal_path = f"{self.config.gold_path}/analytics/temporal"

        analytics_product_files = len([f for f in os.listdir(gold_analytics_product_path) if f.endswith('.parquet')]) if os.path.exists(gold_analytics_product_path) else 0
        analytics_competitive_files = len([f for f in os.listdir(gold_analytics_competitive_path) if f.endswith('.parquet')]) if os.path.exists(gold_analytics_competitive_path) else 0
        analytics_regional_files = len([f for f in os.listdir(gold_analytics_regional_path) if f.endswith('.parquet')]) if os.path.exists(gold_analytics_regional_path) else 0
        analytics_temporal_files = len([f for f in os.listdir(gold_analytics_temporal_path) if f.endswith('.parquet')]) if os.path.exists(gold_analytics_temporal_path) else 0

        if analytics_product_files == 0 and analytics_competitive_files == 0 and analytics_regional_files == 0 and analytics_temporal_files == 0:
            return {"status": "error", "message": "Nenhum arquivo Gold encontrado"}

        return {
            "status": "ok",
            "analytics_product_files": analytics_product_files,
            "analytics_competitive_files": analytics_competitive_files,
            "analytics_regional_files": analytics_regional_files,
            "analytics_temporal_files": analytics_temporal_files,
        }

def main():
    """Função principal para execução do pipeline completo"""

    print("🎯 PIPELINE DE DADOS - SÉRIE HISTÓRICA DE COMBUSTÍVEIS")
    print("Desafio Técnico SGA - Arquitetura Medalhão")
    print("=" * 70)

    # Criar e executar orquestrador
    orchestrator = PipelineOrchestrator()

    # Opção de validação prévia
    validate_first = input("🔍 Executar validação prévia? (y/n): ").lower().strip()

    if validate_first == 'y':
        print("\n🔍 Executando validação prévia...")
        validator = DataQualityValidator()
        validation_results = validator.validate_pipeline_flow()
        print(f"Status validação: {validation_results['overall_status']}")

    # Confirmar execução
    proceed = input("\n🚀 Prosseguir com execução completa do pipeline? (y/n): ").lower().strip()

    if proceed != 'y':
        print("❌ Execução cancelada pelo usuário.")
        return

    # Executar pipeline completo
    result = orchestrator.run_full_pipeline()

    return result

if __name__ == "__main__":
    main()
