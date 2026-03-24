import pandas as pd

df = pd.read_csv("acidentes_rodovias.csv")

print(df.head())

#Estrutura das colunas e tipos de dados
df.info()

#contagem de valores nulos por coluna
df.isnull().sum()

#Estatísticas descritivas para colunas numéricas
df.describe()

#Padronização de nomes de colunas (exemplo: remover espaços, usar snake_case)
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

#Conversão de tipos de dados (exemplo: converter datas para datetime)
#df['data'] = pd.to_datetime(df['data'])
#df['km'] = df['km'].astype(str)