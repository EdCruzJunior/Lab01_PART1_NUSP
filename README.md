# Projeto ETL – Acidentes em Rodovias

## 1. Arquitetura

Este projeto implementa um **pipeline ETL** para ingestão, tratamento e armazenamento de dados de acidentes em rodovias utilizando Python, formato Parquet e PostgreSQL.

### Fluxo de Dados

```
Fonte de Dados (CSV / Dataset Original)
        ↓
Python ETL (Limpeza e Transformação)
        ↓
Camada Silver (Arquivo Parquet)
        ↓
Carga para Data Warehouse
(PostgreSQL - Star Schema)
```

### Descrição das Camadas ####

| Camada     | Descrição                                                    |
| ---------- | ------------------------------------------------------------ |
| Fonte      | Arquivo original contendo os registros de acidentes          |
| Python     | Scripts responsáveis pela limpeza, transformação e modelagem |
| Silver     | Dados estruturados armazenados em formato **Parquet**        |
| PostgreSQL | Banco de dados analítico com modelo **Star Schema**          |

---

# 2. Documentação da Tarefa

## Etapa 1 – Coleta dos Dados ####

O dataset **Acidentes_Rodovias** foi utilizado como fonte inicial contendo registros de acidentes em rodovias.

**Script utilizado**

```
import_open_data.py -- Importe dos dados abertos remotamente
Carregamento_dados.py -- carregando os arquivos para leitura dos datasets
```

Função principal:

* leitura do dataset original
* validação inicial das colunas
* salvamento inicial na camada Bronze

Exemplo:

```python
df = pd.read_csv("acidentes_rodovias.csv")
```

<img width="1382" height="646" alt="image" src="https://github.com/user-attachments/assets/0a130188-638b-433e-b21c-b5d4b43121dc" />


---

## Etapa 2 – Limpeza e Padronização (Camada Silver)

Os dados foram tratados e convertidos para **Parquet**, formato otimizado para analytics.

Principais transformações:

* padronização de nomes de colunas
* remoção de duplicidades
* tratamento de valores nulos
* conversão de tipos

Script:

```
transform_silver.py
```

Exemplo:

```python
df.columns = df.columns.str.lower()
df.to_parquet("data/silver/acidentes.parquet")
```

<img width="1485" height="518" alt="image" src="https://github.com/user-attachments/assets/92abccc2-4d9d-4da4-8a73-bd46b28d3dd9" />


---

## Etapa 3 – Modelagem Analítica

Foi criado um **Star Schema** para permitir consultas analíticas rápidas.

### Tabelas

**Dimensões**

```
dim_tempo
dim_localizacao
dim_acidente
```

**Tabela Fato**

```
fact_acidentes
```

📸 Inserir print da criação das tabelas no PostgreSQL

---

## Etapa 4 – ETL para PostgreSQL

O script ETL:

* lê o **Parquet**
* cria **surrogate keys**
* popula **dimensões**
* carrega a **fact table**

Script:

```
etl_load_dw_Gold.py
```

Tecnologias utilizadas:

* Python
* Apache Arrow
* COPY do PostgreSQL

📸 Inserir print da execução do ETL

---

# 3. Dicionário de Dados

Tabela base: **Acidentes_Rodovias**

| Coluna                | Tipo    | Descrição                       |
| --------------------- | ------- | ------------------------------- |
| data                  | date    | Data do acidente                |
| hora                  | time    | Horário do acidente             |
| rodovia               | varchar | Rodovia onde ocorreu o acidente |
| km                    | numeric | Quilômetro da rodovia           |
| sentido               | varchar | Sentido da via                  |
| municipio             | varchar | Município do acidente           |
| regiao_administrativa | varchar | Região administrativa           |
| latitude              | numeric | Latitude geográfica             |
| longitude             | numeric | Longitude geográfica            |
| classe                | varchar | Classe do acidente              |
| subclasse             | varchar | Subclasse do acidente           |
| causa_provavel        | varchar | Causa provável do acidente      |
| veiculos_envolvidos   | varchar | Quantidade e tipo de veículos   |
| vitima_ilesa          | integer | Número de vítimas ilesas        |
| vitima_leve           | integer | Número de vítimas leves         |
| vitima_moderada       | integer | Número de vítimas moderadas     |
| vitima_grave          | integer | Número de vítimas graves        |
| vitima_fatal          | integer | Número de vítimas fatais        |

---

# 4. Qualidade de Dados

Durante a análise dos dados foram identificados alguns problemas.

## Valores Nulos

| Coluna         | % Nulos |
| -------------- | ------- |
| causa_provavel | ~12%    |
| subclasse      | ~8%     |
| sentido        | ~3%     |

Tratamento aplicado:

* substituição por `"Não Informado"`
* ou valor padrão

---

## Coluna com Estrutura Complexa

A coluna **veiculos_envolvidos** apresenta valores como:

```
AUTOMÓVEL=1|MOTO=1
CAMINHÃO=1|AUTOMÓVEL=2
```

Foi criada uma função para extrair o **total de veículos envolvidos**.

---

## Tipos de Dados Inconsistentes

Algumas colunas numéricas estavam como **string**.

Tratamento:

```
conversão para inteiro
substituição de valores inválidos por 0
```

---

# 5. Instruções de Execução

## 1 – Clonar o projeto

```
git clone <repositorio>
cd projeto_acidentes
```

---

## 2 – Criar ambiente virtual

```
python -m venv venv
source venv/bin/activate
```

Windows:

```
venv\Scripts\activate
```

---

## 3 – Instalar dependências

```
pip install -r requirements.txt
```

Exemplo de `requirements.txt`

```
pandas
pyarrow
psycopg2-binary
sqlalchemy
```

---

## 4 – Executar os scripts na ordem

### 1️⃣ Transformação para Silver

```
python transform_silver.py
```

Gera:

```
data/silver/acidentes.parquet
```

---

### 2️⃣ Criar estrutura do Data Warehouse

```
python create_schema.py
```

Cria:

```
dim_tempo
dim_localizacao
dim_acidente
fact_acidentes
```

---

### 3️⃣ Executar ETL

```
python etl_load_dw.py
```

Este script:

* lê o parquet
* cria dimensões
* gera surrogate keys
* carrega fact table

---

# Resultado Final

Banco PostgreSQL com **Star Schema otimizado para análise de acidentes rodoviários**.

Consultas analíticas possíveis:

* rodovias com maior número de acidentes
* horários mais perigosos
* municípios com mais vítimas fatais
* principais causas de acidentes

---

# Tecnologias Utilizadas

* Python
* PostgreSQL
* Apache Arrow
* Parquet
* SQL
* ETL

---

# Estrutura do Projeto

```
project
│
├── data
│   ├── bronze
│   ├── silver
│   │    └── acidentes.parquet
│
├── scripts
│   ├── transform_silver.py
│   ├── create_schema.py
│   └── etl_load_dw.py
│
├── requirements.txt
└── README.md
```
