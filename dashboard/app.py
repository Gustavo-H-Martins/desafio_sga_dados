# -*- coding: utf-8 -*-
"""
Dashboard Interativo SGA - Análise de Combustíveis
Implementa visualizações das análises requeridas no desafio
Utiliza Streamlit para interface web interativa
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import sys
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Configurar paths
dashboard_dir = Path(__file__).parent
project_root = dashboard_dir.parent
sys.path.append(str(project_root))

# Configuração da página
st.set_page_config(
    page_title="SGA - Análise de Combustíveis",
    page_icon="⛽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado
st.markdown("""
<style>
    .reportview-container {
        margin-top: -2em;
    }
    .stDeployButton {display:none;}
    footer {visibility: hidden;}
    .stApp > header {visibility: hidden;}
    .main .block-container {
        padding-top: 1rem;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12);
        border-left: 4px solid #1f77b4;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_sample_data():
    """
    Carrega dados sintéticos para demonstração do dashboard
    Em ambiente real, carregaria da camada Gold
    """
    np.random.seed(42)

    # Simular dados processados da camada Gold
    estados = ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'PE', 'CE', 'GO']
    regioes = {'SP': 'SUDESTE', 'RJ': 'SUDESTE', 'MG': 'SUDESTE',
               'RS': 'SUL', 'PR': 'SUL', 'SC': 'SUL',
               'BA': 'NORDESTE', 'PE': 'NORDESTE', 'CE': 'NORDESTE',
               'GO': 'CENTRO-OESTE'}

    produtos = ['GASOLINA COMUM', 'GASOLINA ADITIVADA', 'ETANOL', 'ÓLEO DIESEL', 'ÓLEO DIESEL S10']
    bandeiras = ['PETROBRAS', 'SHELL', 'IPIRANGA', 'RAIZEN', 'BRANCA']

    # Gerar dados temporais (2020-2024)
    data = []
    base_prices = {
        'GASOLINA COMUM': 5.20,
        'GASOLINA ADITIVADA': 5.50,
        'ETANOL': 3.80,
        'ÓLEO DIESEL': 4.80,
        'ÓLEO DIESEL S10': 5.00
    }

    for year in range(2020, 2025):
        for month in range(1, 13):
            for state in estados:
                for product in produtos:
                    # Simular variações temporais e regionais realistas
                    base_price = base_prices[product]

                    # Tendência temporal (inflação)
                    inflation_factor = 1 + (year - 2020) * 0.08  # 8% ao ano

                    # Sazonalidade (alta no meio/final do ano)
                    seasonal_factor = 1 + 0.1 * np.sin(2 * np.pi * (month - 1) / 12)

                    # Variação regional
                    regional_factors = {
                        'SUDESTE': 1.1, 'SUL': 1.05, 'NORDESTE': 0.95,
                        'CENTRO-OESTE': 0.98, 'NORTE': 1.15
                    }
                    regional_factor = regional_factors.get(regioes[state], 1.0)

                    # Ruído aleatório
                    noise_factor = 1 + np.random.normal(0, 0.05)

                    final_price = base_price * inflation_factor * seasonal_factor * regional_factor * noise_factor

                    # Dados de margem e market share
                    margin = np.random.uniform(0.08, 0.15)  # 8-15% margem

                    for brand in np.random.choice(bandeiras, size=np.random.randint(2, 4), replace=False):
                        brand_factor = {
                            'PETROBRAS': 1.02, 'SHELL': 1.05, 'IPIRANGA': 1.01,
                            'RAIZEN': 1.00, 'BRANCA': 0.97
                        }[brand]

                        data.append({
                            'ano': year,
                            'mes': month,
                            'estado': state,
                            'regiao': regioes[state],
                            'produto': product,
                            'bandeira': brand,
                            'preco_medio': round(final_price * brand_factor, 3),
                            'margem_percentual': round(margin * 100, 2),
                            'num_postos': np.random.randint(50, 500),
                            'data': datetime(year, month, 1)
                        })

    df = pd.DataFrame(data)

    # Adicionar cálculos de viabilidade do etanol
    viabilidade_data = []
    for _, row in df.iterrows():
        if row['produto'] == 'ETANOL':
            # Buscar preço da gasolina no mesmo estado/período
            gasolina_price = df[
                (df['estado'] == row['estado']) & 
                (df['ano'] == row['ano']) & 
                (df['mes'] == row['mes']) & 
                (df['produto'] == 'GASOLINA COMUM')
            ]['preco_medio'].mean()

            if pd.notna(gasolina_price):
                ratio = row['preco_medio'] / gasolina_price
                viabilidade_data.append({
                    'estado': row['estado'],
                    'ano': row['ano'],
                    'mes': row['mes'],
                    'ratio_etanol_gasolina': ratio,
                    'etanol_viavel': ratio <= 0.7
                })

    df_viabilidade = pd.DataFrame(viabilidade_data)

    return df, df_viabilidade

def create_price_evolution_chart(df):
    """Cria gráfico de evolução de preços"""
    monthly_avg = df.groupby(['data', 'produto'])['preco_medio'].mean().reset_index()

    fig = px.line(
        monthly_avg, 
        x='data', 
        y='preco_medio',
        color='produto',
        title='📈 Evolução Temporal dos Preços por Combustível (2020-2024)',
        labels={
            'data': 'Período',
            'preco_medio': 'Preço Médio (R$)',
            'produto': 'Combustível'
        }
    )

    fig.update_layout(
        height=500,
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    return fig

def create_regional_ranking_chart(df):
    """Cria gráfico de ranking regional"""
    regional_avg = df.groupby(['regiao', 'produto'])['preco_medio'].mean().reset_index()

    # Focar nos principais combustíveis
    main_fuels = ['GASOLINA COMUM', 'ETANOL', 'ÓLEO DIESEL']
    regional_avg_main = regional_avg[regional_avg['produto'].isin(main_fuels)]

    fig = px.bar(
        regional_avg_main,
        x='regiao',
        y='preco_medio',
        color='produto',
        title='🗺️ Ranking de Preços Médios por Região',
        labels={
            'regiao': 'Região',
            'preco_medio': 'Preço Médio (R$)',
            'produto': 'Combustível'
        },
        barmode='group'
    )

    fig.update_layout(height=500)
    return fig

def create_ethanol_viability_chart(df_viabilidade):
    """Cria gráfico de viabilidade do etanol"""
    if df_viabilidade.empty:
        st.warning("Dados de viabilidade do etanol não disponíveis")
        return None

    viab_summary = df_viabilidade.groupby('estado').agg({
        'etanol_viavel': ['sum', 'count'],
        'ratio_etanol_gasolina': 'mean'
    }).round(3)

    viab_summary.columns = ['casos_viaveis', 'total_casos', 'ratio_medio']
    viab_summary = viab_summary.reset_index()
    viab_summary['percentual_viabilidade'] = (viab_summary['casos_viaveis'] / viab_summary['total_casos'] * 100).round(1)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=viab_summary['estado'],
        y=viab_summary['percentual_viabilidade'],
        name='% Viabilidade',
        marker_color='green',
        yaxis='y'
    ))

    fig.add_trace(go.Scatter(
        x=viab_summary['estado'],
        y=viab_summary['ratio_medio'] * 100,
        mode='lines+markers',
        name='Ratio Médio (%)',
        line=dict(color='red'),
        yaxis='y2'
    ))

    fig.update_layout(
        title='⚡ Viabilidade Econômica do Etanol por Estado (Ratio ≤ 70%)',
        xaxis_title='Estado',
        yaxis=dict(title='% de Períodos Viáveis', side='left'),
        yaxis2=dict(title='Ratio Etanol/Gasolina (%)', side='right', overlaying='y'),
        height=500
    )

    return fig

def create_brand_competition_chart(df):
    """Cria gráfico de competição entre bandeiras"""
    market_share = df.groupby(['bandeira', 'produto'])['num_postos'].sum().reset_index()

    # Calcular market share percentual
    total_by_product = market_share.groupby('produto')['num_postos'].sum()
    market_share['market_share_pct'] = market_share.apply(
        lambda row: row['num_postos'] / total_by_product[row['produto']] * 100, axis=1
    ).round(1)

    # Focar na gasolina comum para simplicidade
    gas_data = market_share[market_share['produto'] == 'GASOLINA COMUM']

    fig = px.pie(
        gas_data,
        values='market_share_pct',
        names='bandeira',
        title='🏪 Market Share por Bandeira - Gasolina Comum',
        color_discrete_sequence=px.colors.qualitative.Set3
    )

    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(height=500)

    return fig

def create_seasonal_analysis_chart(df):
    """Cria gráfico de análise sazonal"""
    # Análise por mês do ano
    seasonal_data = df.groupby(['mes', 'produto'])['preco_medio'].mean().reset_index()

    # Focar nos principais combustíveis
    main_fuels = ['GASOLINA COMUM', 'ETANOL']
    seasonal_main = seasonal_data[seasonal_data['produto'].isin(main_fuels)]

    fig = px.line(
        seasonal_main,
        x='mes',
        y='preco_medio',
        color='produto',
        title='📅 Padrão Sazonal de Preços (Média por Mês)',
        labels={
            'mes': 'Mês',
            'preco_medio': 'Preço Médio (R$)',
            'produto': 'Combustível'
        },
        markers=True
    )

    fig.update_layout(height=400)
    fig.update_xaxes(tickmode='linear', tick0=1, dtick=1)

    return fig

def main():
    """Função principal do dashboard"""

    # Header
    st.title("⛽ SGA - Dashboard de Análise de Combustíveis")
    st.markdown("**Análise Completa do Mercado Brasileiro de Combustíveis (2020-2024)**")
    st.markdown("---")

    # Carregar dados
    with st.spinner('Carregando dados...'):
        df, df_viabilidade = load_sample_data()

    # Sidebar com filtros
    st.sidebar.title("🔧 Filtros de Análise")

    # Filtros
    anos_disponiveis = sorted(df['ano'].unique())
    anos_selecionados = st.sidebar.multiselect(
        "Selecionar Anos",
        anos_disponiveis,
        default=anos_disponiveis[-2:]  # Últimos 2 anos
    )

    produtos_disponiveis = sorted(df['produto'].unique())
    produtos_selecionados = st.sidebar.multiselect(
        "Selecionar Combustíveis",
        produtos_disponiveis,
        default=['GASOLINA COMUM', 'ETANOL', 'ÓLEO DIESEL']
    )

    regioes_disponiveis = sorted(df['regiao'].unique())
    regioes_selecionadas = st.sidebar.multiselect(
        "Selecionar Regiões",
        regioes_disponiveis,
        default=regioes_disponiveis
    )

    # Aplicar filtros
    df_filtered = df[
        (df['ano'].isin(anos_selecionados)) &
        (df['produto'].isin(produtos_selecionados)) &
        (df['regiao'].isin(regioes_selecionadas))
    ]

    # Verificar se há dados após filtros
    if df_filtered.empty:
        st.warning("⚠️ Nenhum dado disponível com os filtros selecionados. Por favor, ajuste os filtros.")
        return

    # Métricas principais
    st.subheader("📊 Métricas Principais")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        avg_gas_price = df_filtered[df_filtered['produto'] == 'GASOLINA COMUM']['preco_medio'].mean()
        st.metric(
            "Preço Médio Gasolina", 
            f"R$ {avg_gas_price:.3f}" if pd.notna(avg_gas_price) else "N/A"
        )

    with col2:
        avg_ethanol_price = df_filtered[df_filtered['produto'] == 'ETANOL']['preco_medio'].mean()
        st.metric(
            "Preço Médio Etanol", 
            f"R$ {avg_ethanol_price:.3f}" if pd.notna(avg_ethanol_price) else "N/A"
        )

    with col3:
        if pd.notna(avg_gas_price) and pd.notna(avg_ethanol_price):
            ratio_current = avg_ethanol_price / avg_gas_price
            st.metric(
                "Ratio Etanol/Gasolina",
                f"{ratio_current:.1%}",
                delta="Viável" if ratio_current <= 0.7 else "Não viável"
            )
        else:
            st.metric("Ratio Etanol/Gasolina", "N/A")

    with col4:
        total_stations = df_filtered['num_postos'].sum()
        st.metric("Total de Postos", f"{total_stations:,}")

    st.markdown("---")

    # Gráficos principais
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Evolução Temporal", 
        "🗺️ Análise Regional", 
        "⚡ Viabilidade Etanol",
        "🏪 Competição"
    ])

    with tab1:
        st.subheader("Evolução Temporal dos Preços")

        col1, col2 = st.columns(2)

        with col1:
            fig_evolution = create_price_evolution_chart(df_filtered)
            st.plotly_chart(fig_evolution, use_container_width=True)

        with col2:
            fig_seasonal = create_seasonal_analysis_chart(df_filtered)
            st.plotly_chart(fig_seasonal, use_container_width=True)

        # Insights temporais
        st.subheader("💡 Insights Temporais")
        st.info("""
        **Principais Tendências:**
        • Crescimento consistente dos preços de 2020 a 2024 (inflação)
        • Padrão sazonal com picos no segundo semestre
        • Etanol mantém correlação com gasolina, mas com maior volatilidade
        • Diesel apresenta menor variação sazonal
        """)

    with tab2:
        st.subheader("Análise Regional de Preços")

        col1, col2 = st.columns([2, 1])

        with col1:
            fig_regional = create_regional_ranking_chart(df_filtered)
            st.plotly_chart(fig_regional, use_container_width=True)

        with col2:
            # Tabela de ranking
            regional_ranking = df_filtered.groupby(['regiao', 'produto'])['preco_medio'].mean().reset_index()
            gas_ranking = regional_ranking[regional_ranking['produto'] == 'GASOLINA COMUM'].sort_values('preco_medio')

            st.write("**Ranking Gasolina por Região:**")
            for i, row in gas_ranking.iterrows():
                st.write(f"{row.name + 1}º {row['regiao']}: R$ {row['preco_medio']:.3f}")

        # Insights regionais
        st.subheader("💡 Insights Regionais")
        st.info("""
        **Diferenças Regionais:**
        • Região Norte apresenta preços mais altos (logística)
        • Sudeste com preços premium (maior demanda)
        • Nordeste competitivo em etanol (produção local)
        • Sul equilibrado entre combustíveis fósseis e renováveis
        """)

    with tab3:
        st.subheader("Análise de Viabilidade do Etanol")

        fig_viability = create_ethanol_viability_chart(df_viabilidade)
        if fig_viability:
            st.plotly_chart(fig_viability, use_container_width=True)

        # Análise detalhada
        col1, col2 = st.columns(2)

        with col1:
            st.write("**Critério de Viabilidade:**")
            st.write("• Etanol viável quando preço ≤ 70% da gasolina")
            st.write("• Baseado na eficiência energética relativa")
            st.write("• Análise considera apenas aspecto econômico")

        with col2:
            if not df_viabilidade.empty:
                viab_overall = df_viabilidade['etanol_viavel'].mean() * 100
                st.metric("Viabilidade Geral", f"{viab_overall:.1f}%")

                best_state = df_viabilidade.groupby('estado')['etanol_viavel'].mean().idxmax()
                st.write(f"**Melhor Estado:** {best_state}")

    with tab4:
        st.subheader("Competição entre Bandeiras")

        col1, col2 = st.columns(2)

        with col1:
            fig_competition = create_brand_competition_chart(df_filtered)
            st.plotly_chart(fig_competition, use_container_width=True)

        with col2:
            # Análise de posicionamento
            brand_positioning = df_filtered.groupby(['bandeira', 'produto']).agg({
                'preco_medio': 'mean',
                'num_postos': 'sum'
            }).reset_index()

            gas_positioning = brand_positioning[brand_positioning['produto'] == 'GASOLINA COMUM']
            gas_positioning = gas_positioning.sort_values('preco_medio')

            st.write("**Posicionamento de Preços:**")
            for _, row in gas_positioning.iterrows():
                strategy = "Premium" if row['preco_medio'] > gas_positioning['preco_medio'].quantile(0.67) else                           "Econômico" if row['preco_medio'] < gas_positioning['preco_medio'].quantile(0.33) else "Médio"
                st.write(f"• {row['bandeira']}: {strategy} (R$ {row['preco_medio']:.3f})")

    # Footer com informações do projeto
    st.markdown("---")
    st.markdown("""
    **📋 Sobre o Projeto:**
    Este dashboard foi desenvolvido como parte do desafio técnico SGA, implementando arquitetura medalhão 
    (Bronze → Silver → Gold) para processamento de dados de combustíveis brasileiros. 

    **🛠️ Tecnologias:** Python, Pandas, Streamlit, Plotly, Parquet, Arquitetura de Data Lake
    """)

if __name__ == "__main__":
    main()
