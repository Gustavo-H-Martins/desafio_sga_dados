#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para executar o dashboard SGA
"""

import subprocess
import sys
from pathlib import Path

def run_dashboard():
    """Executa o dashboard Streamlit"""
    dashboard_dir = Path(__file__).parent
    app_file = dashboard_dir / "app.py"

    if not app_file.exists():
        print(f"❌ Arquivo app.py não encontrado em: {app_file}")
        sys.exit(1)

    print("🚀 Iniciando dashboard SGA...")
    print("📊 Acesse: http://localhost:8501")
    print("⏹️  Para parar: Ctrl+C")

    # Executar Streamlit
    subprocess.run([
        sys.executable, "-m", "streamlit", "run", str(app_file),
        "--server.port", "8501",
        "--server.address", "0.0.0.0",
        "--theme.base", "light"
    ])

if __name__ == "__main__":
    run_dashboard()
