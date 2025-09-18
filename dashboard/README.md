# Dashboard SGA - Análise de Combustíveis

Dashboard interativo desenvolvido com Streamlit para visualização das análises de combustíveis brasileiros.

## 🚀 Como Executar

```bash
# Instalar dependências
pip install -r requirements.txt

# Executar dashboard
python run_dashboard.py

# Ou diretamente com Streamlit
streamlit run app.py
```

## 📊 Funcionalidades

### Filtros Interativos
- **Anos:** Selecionar período de análise (2020-2024)
- **Combustíveis:** Filtrar por tipo de combustível
- **Regiões:** Análise regional específica

### Visualizações

#### 📈 Evolução Temporal
- Evolução de preços por combustível ao longo do tempo
- Análise de sazonalidade mensal
- Tendências e padrões temporais

#### 🗺️ Análise Regional
- Ranking de preços por região
- Comparação regional entre combustíveis
- Identificação de disparidades

#### ⚡ Viabilidade do Etanol
- Análise econômica etanol vs gasolina
- Critério de viabilidade (ratio ≤ 70%)
- Viabilidade por estado e período

#### 🏪 Competição de Mercado
- Market share por bandeira
- Posicionamento de preços (Premium/Médio/Econômico)
- Análise competitiva

## 🛠️ Tecnologias

- **Streamlit:** Interface web interativa
- **Plotly:** Gráficos interativos
- **Pandas:** Manipulação de dados
- **NumPy:** Computação científica

## 📁 Estrutura

```
dashboard/
├── app.py              # Aplicação principal
├── run_dashboard.py    # Script de execução
├── requirements.txt    # Dependências
└── README.md          # Este arquivo
```
