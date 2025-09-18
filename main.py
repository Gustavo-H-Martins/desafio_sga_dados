#!/usr/bin/env python3
"""
Script Principal - Pipeline de Dados SGA
Desafio Técnico - Série Histórica de Preços de Combustíveis

Executa o pipeline completo seguindo arquitetura medalhão:
Bronze (dados brutos) → Silver (processados) → Gold (analytics)
"""

import sys
import os
from datetime import datetime

# Adicionar path do projeto
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

def main():
    """Função principal do projeto"""

    print("🛢️" + "=" * 60)
    print("  PIPELINE DE DADOS - SÉRIE HISTÓRICA DE COMBUSTÍVEIS")
    print("  Desafio Técnico SGA - Engenheiro de Dados Sênior")
    print("  Arquitetura Medalhão (Bronze → Silver → Gold)")
    print("=" * 62)
    print(f"📅 Execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📂 Projeto: {project_root}")
    print()

    # Menu de opções
    print("🎯 OPÇÕES DISPONÍVEIS:")
    print("1. 🚀 Executar pipeline completo (Bronze → Silver → Gold)")
    print("2. 📊 Executar camada Bronze apenas")
    print("3. 🔧 Executar camada Silver apenas") 
    print("4. 🏆 Executar camada Gold apenas")
    print("5. 🔍 Validar pipeline existente")
    print("6. 📱 Executar dashboard")
    print("7. 📓 Abrir notebook de demonstração (funcional em ambiente linux)")
    print("8. 🆘 Ajuda e documentação")
    print("0. ❌ Sair")
    print()

    while True:
        try:
            choice = input("👉 Escolha uma opção (0-8): ").strip()

            if choice == "0":
                print("👋 Encerrando...")
                break

            elif choice == "1":
                execute_full_pipeline()

            elif choice == "2":
                execute_bronze_only()

            elif choice == "3":
                execute_silver_only()

            elif choice == "4":
                execute_gold_only()

            elif choice == "5":
                validate_pipeline()

            elif choice == "6":
                run_dashboard()

            elif choice == "7":
                open_notebook()

            elif choice == "8":
                show_help()

            else:
                print("❌ Opção inválida. Tente novamente.")

        except KeyboardInterrupt:
            print("\n👋 Execução interrompida pelo usuário.")
            break
        except Exception as e:
            print(f"❌ Erro: {e}")

def execute_full_pipeline():
    """Executa pipeline completo"""
    print("\n🚀 Executando pipeline completo...")

    try:
        from jobs.orchestration.pipeline_orchestrator import PipelineOrchestrator

        orchestrator = PipelineOrchestrator()
        result = orchestrator.run_full_pipeline()

        if result['status'] == 'completed':
            print("\n✅ Pipeline executado com sucesso!")
            print("💡 Execute a opção 6 para ver o dashboard")
        else:
            print(f"\n❌ Pipeline falhou: {result.get('errors', [])}")

    except Exception as e:
        print(f"❌ Erro na execução: {e}")

def execute_bronze_only():
    """Executa apenas camada Bronze"""
    print("\n📊 Executando camada Bronze...")

    try:
        from jobs.bronze_layer.bronze_ingestion import BronzeIngestionJob

        job = BronzeIngestionJob()
        result = job.execute()

        print(f"Status: {result['status']}")
        print(f"Registros: {result.get('total_records', 0):,}")

    except Exception as e:
        print(f"❌ Erro: {e}")

def execute_silver_only():
    """Executa apenas camada Silver"""
    print("\n🔧 Executando camada Silver...")

    try:
        from jobs.silver_layer.silver_transformation import SilverTransformationJob

        job = SilverTransformationJob()
        result = job.execute()

        print(f"Status: {result['status']}")
        print(f"Entrada: {result.get('records_input', 0):,}")
        print(f"Saída: {result.get('records_output', 0):,}")

    except Exception as e:
        print(f"❌ Erro: {e}")

def execute_gold_only():
    """Executa apenas camada Gold"""
    print("\n🏆 Executando camada Gold...")

    try:
        from jobs.gold_layer.gold_analytics import GoldAnalyticsJob

        job = GoldAnalyticsJob()

        result = job.execute()

        print(f"Status: {result['status']}")
        print(f"Analytics: {result.get('analyses_generated', [])}")
        print(f"Datasets: {len(result.get('categories', []))}")

    except Exception as e:
        print(f"❌ Erro: {e}")

def validate_pipeline():
    """Valida pipeline existente"""
    print("\n🔍 Validando pipeline...")

    try:
        from jobs.orchestration.pipeline_orchestrator import DataQualityValidator

        validator = DataQualityValidator()
        result = validator.validate_pipeline_flow()

        print(f"Status geral: {result['overall_status']}")

        for layer, validation in result['validations'].items():
            status_icon = "✅" if validation['status'] == 'ok' else "❌"
            print(f"{status_icon} {layer.upper()}: {validation.get('message', 'OK')}")

    except Exception as e:
        print(f"❌ Erro: {e}")

def run_dashboard():
    """Executa dashboard"""
    print("\n📱 Iniciando dashboard...")
    print("💡 Acesse: http://localhost:8501")
    print("🛑 Para parar: Ctrl+C")

    try:
        import subprocess
        cmd = [sys.executable, "dashboard/run_dashboard.py"]
        subprocess.run(cmd, cwd=project_root)

    except KeyboardInterrupt:
        print("\n✅ Dashboard encerrado.")
    except Exception as e:
        print(f"❌ Erro: {e}")

def open_notebook():
    """Abre notebook de demonstração PARA EXECUTAR EM AMBIENTE LINUX"""
    notebook_path = os.path.join(project_root, "notebooks", "pipeline_execution_demo.ipynb")

    print(f"\n📓 Notebook: {notebook_path}")
    print("💡 Abra este arquivo em Jupyter Lab/Notebook")

    # Tentar abrir automaticamente se Jupyter estiver disponível
    try:
        import subprocess
        subprocess.run(["jupyter", "lab", notebook_path], check=False)
    except:
        print("⚠️ Jupyter não encontrado. Abra manualmente o arquivo acima.")

def show_help():
    """Mostra ajuda"""
    print("\n🆘 AJUDA E DOCUMENTAÇÃO")
    print("=" * 40)
    print()
    print("📋 ESTRUTURA DO PROJETO:")
    print("├── datalake/           # Data Lake (Bronze, Silver, Gold)")
    print("├── jobs/              # Jobs de processamento")
    print("├── config/            # Configurações")
    print("├── utils/             # Utilitários")
    print("├── dashboard/         # Dashboard Streamlit")
    print("├── notebooks/         # Notebooks Jupyter")
    print("└── docs/              # Documentação")
    print()
    print("🎯 CAMADAS DO PIPELINE:")
    print("• Bronze: Dados brutos (CSV → Parquet)")
    print("• Silver: Dados limpos e normalizados")
    print("• Gold: Analytics e agregações para consumo")
    print()
    print("📊 ANÁLISES DISPONÍVEIS:")
    print("• Evolução temporal de preços")
    print("• Ranking regional de custos")
    print("• Competitividade entre bandeiras")
    print("• Viabilidade econômica etanol vs gasolina")
    print()
    print("🔗 RECURSOS:")
    print(f"• README: {project_root}/README.md")
    print(f"• Dashboard: {project_root}/dashboard/")
    print(f"• Notebook: {project_root}/notebooks/")
    print("• Fonte dados: https://dados.gov.br/")

if __name__ == "__main__":
    main()
