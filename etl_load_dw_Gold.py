pip install pandas pyarrow psycopg2-binary

#Funcão criada para conversão de dados

def parse_veiculos(valor):

    if valor is None:
        return 0

    if isinstance(valor, int):
        return valor

    total = 0

    partes = str(valor).split("|")

    for p in partes:
        if "=" in p:
            total += int(p.split("=")[1])

    return total

    # Carga de dados para o PostgreSQL

    import pyarrow.parquet as pq
import psycopg2
from io import StringIO
import csv

PARQUET_FILE = "data/silver/acidentes_rodovias.parquet"

DB_CONFIG = {
    "host": "localhost",
    "database": "Acidentes_Rodovias",
    "user": "postgres",
    "password": "Elefante@2409",
    "port": 5432
}

SCHEMA = "dw_acidentes"

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

print("Conectado ao PostgreSQL")

# caches para surrogate keys
dim_tempo = {}
dim_local = {}
dim_acidente = {}

parquet = pq.ParquetFile(PARQUET_FILE)

for batch in parquet.iter_batches(batch_size=20000):

    fact_buffer = StringIO()
    fact_writer = csv.writer(fact_buffer)

    table = batch.to_pydict()

    n = len(table["data"])

    for i in range(n):

        data = table["data"][i]
        hora = table["hora"][i]

        rodovia = table["rodovia"][i]
        km = table["km"][i]
        sentido = table["sentido"][i]

        classe = table["classe"][i]
        subclasse = table["subclasse"][i]

        veiculos = table["veiculos_envolvidos"][i]
        ilesos = table["vitima_ilesa"][i]
        leves = table["vitima_leve"][i]
        moderados = table["vitima_moderada"][i]
        graves = table["vitima_grave"][i]
        fatais = table["vitima_fatal"][i]

        # ======================
        # DIM TEMPO
        # ======================

        tempo_key = (data, hora)

        if tempo_key not in dim_tempo:

            cursor.execute(f"""
            INSERT INTO {SCHEMA}.dim_tempo (data,hora)
            VALUES (%s,%s)
            RETURNING id_tempo
            """, (data, hora))

            dim_tempo[tempo_key] = cursor.fetchone()[0]

        # ======================
        # DIM LOCALIZAÇÃO
        # ======================

        local_key = (rodovia, km, sentido)

        if local_key not in dim_local:

            cursor.execute(f"""
            INSERT INTO {SCHEMA}.dim_localizacao
            (rodovia,km,sentido)
            VALUES (%s,%s,%s)
            RETURNING id_localizacao
            """, (rodovia, km, sentido))

            dim_local[local_key] = cursor.fetchone()[0]

        # ======================
        # DIM ACIDENTE
        # ======================

        acidente_key = (classe, subclasse)

        if acidente_key not in dim_acidente:

            cursor.execute(f"""
            INSERT INTO {SCHEMA}.dim_acidente
            (classe,subclasse)
            VALUES (%s,%s)
            RETURNING id_acidente_tipo
            """, (classe, subclasse))

            dim_acidente[acidente_key] = cursor.fetchone()[0]

        # ======================
        # FACT TABLE
        # ======================
       

        fact_writer.writerow([
            dim_tempo[tempo_key],
            dim_local[local_key],
            dim_acidente[acidente_key],
            parse_veiculos(veiculos),
            int(ilesos or 0),
            int(leves or 0),
            int(moderados or 0),
            int(graves or 0),
            int(fatais or 0)
        ])

    # enviar batch para postgres
    fact_buffer.seek(0)

    cursor.copy_expert(f"""
    COPY {SCHEMA}.fact_acidentes
    (
        id_tempo,
        id_localizacao,
        id_acidente_tipo,
        veiculos_envolvidos,
        vitima_ilesa,
        vitima_leve,
        vitima_moderada,
        vitima_grave,
        vitima_fatal
    )
    FROM STDIN WITH CSV
    """, fact_buffer)

    conn.commit()

    print("Batch carregado")

cursor.close()
conn.close()

print("ETL finalizado com sucesso")