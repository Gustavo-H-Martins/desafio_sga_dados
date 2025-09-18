# 📚 Dicionário de Dados

## Dados de Origem (Bronze)

### Tabela: combustiveis_raw

| Campo | Tipo | Descrição | Exemplo |
|-------|------|-----------|---------|
| `regiao_sigla` | string | Sigla da região geográfica | "SE", "NE", "S" |
| `estado_sigla` | string | Sigla da unidade federativa | "SP", "RJ", "MG" |
| `municipio` | string | Nome do município | "São Paulo", "Rio de Janeiro" |
| `revenda` | string | Nome fantasia da revenda | "Posto ABC Ltda" |
| `cnpj_revenda` | string | CNPJ da revenda | "12345678901234" |
| `nome_rua` | string | Logradouro da revenda | "Rua das Flores" |
| `numero_rua` | string | Número do endereço | "123", "S/N" |
| `complemento` | string | Complemento do endereço | "Km 15", "Sentido Centro" |
| `bairro` | string | Bairro da revenda | "Centro", "Industrial" |
| `cep` | string | CEP do endereço | "01234567" |
| `produto` | string | Tipo de combustível | "GASOLINA COMUM", "ETANOL" |
| `data_coleta` | date | Data da coleta do preço | "2024-01-15" |
| `valor_venda` | decimal | Preço ao consumidor (R$/L) | 6.25 |
| `valor_compra` | decimal | Preço de aquisição (R$/L) | 5.80 |
| `unidade_medida` | string | Unidade de venda | "R$ / litro" |
| `bandeira` | string | Distribuidora associada | "PETROBRAS", "IPIRANGA" |

## Dados Processados (Silver)

### Tabela: combustiveis_processed

Inclui todos os campos do Bronze mais campos derivados:

| Campo | Tipo | Descrição | Exemplo |
|-------|------|-----------|---------|
| `ano` | integer | Ano da coleta | 2024 |
| `mes` | integer | Mês da coleta | 3 |
| `trimestre` | integer | Trimestre da coleta | 1 |
| `semestre` | integer | Semestre da coleta | 1 |
| `dia_semana` | string | Dia da semana | "Monday" |
| `categoria_produto` | string | Produto categorizado | "GASOLINA", "ETANOL", "DIESEL" |
| `categoria_bandeira` | string | Bandeira categorizada | "PETROBRAS", "BRANCA", "OUTRAS" |
| `margem_absoluta` | decimal | Margem em R$ | 0.45 |
| `margem_percentual` | decimal | Margem percentual | 7.76 |
| `indice_preco_regional` | decimal | Índice vs média regional | 102.5 |

## Dados Analíticos (Gold)

### Dashboard Metrics

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `produto` | string | Produto analisado |
| `preco_medio_atual` | decimal | Preço médio atual |
| `variacao_mensal` | decimal | Variação % mensal |
| `regiao_mais_cara` | string | Região com maior preço |
| `num_observacoes` | integer | Número de observações |

### Evolução Mensal

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `ano` | integer | Ano de referência |
| `mes` | integer | Mês de referência |
| `categoria_produto` | string | Produto |
| `valor_venda_mean` | decimal | Preço médio |
| `variacao_mensal` | decimal | Variação % vs mês anterior |

### Ranking Regional

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `regiao` | string | Região geográfica |
| `categoria_produto` | string | Produto |
| `valor_venda_mean` | decimal | Preço médio |
| `ranking_preco` | integer | Posição no ranking (1=mais caro) |

### Competitividade Bandeiras

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `categoria_bandeira` | string | Bandeira |
| `categoria_produto` | string | Produto |
| `valor_venda_mean` | decimal | Preço médio |
| `market_share` | decimal | Participação de mercado % |
| `ranking_preco` | integer | Ranking de preço |

## Regras de Negócio

### Categorização de Produtos
- **GASOLINA**: "GASOLINA COMUM", "GASOLINA ADITIVADA"
- **ETANOL**: "ETANOL", "ÁLCOOL COMBUSTÍVEL" 
- **DIESEL**: "DIESEL", "DIESEL S10"
- **GLP**: "GLP", "GÁS LIQUEFEITO"

### Categorização de Bandeiras
- **PETROBRAS**: Variações do nome Petrobras
- **IPIRANGA**: Variações do nome Ipiranga
- **SHELL**: Variações do nome Shell
- **RAIZEN**: Variações do nome Raizen
- **BRANCA**: "BANDEIRA BRANCA", postos independentes
- **OUTRAS**: Demais distribuidoras

### Regiões Geográficas
- **N** (Norte): AC, AP, AM, PA, RO, RR, TO
- **NE** (Nordeste): AL, BA, CE, MA, PB, PE, PI, RN, SE
- **CO** (Centro-Oeste): DF, GO, MT, MS
- **SE** (Sudeste): ES, MG, RJ, SP
- **S** (Sul): PR, SC, RS

## Métricas de Qualidade

### Thresholds
- **Completude mínima**: 95%
- **Taxa máxima de duplicatas**: 5%
- **Taxa máxima de outliers**: 10%

### Validações
- **Preços**: Entre R$ 0,10 e R$ 20,00 por litro
- **Datas**: Entre 2020-01-01 e 2024-12-31
- **Regiões**: Apenas valores válidos (N, NE, CO, SE, S)
- **Estados**: Apenas UFs brasileiras válidas
