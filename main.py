import glob
import logging
import os
import shutil
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_user_data.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("EtlUserData")


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    logger.info("Criando/recuperando Spark Session...")
    spark = (
        SparkSession.builder.appName("EtlUserData")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    # Reduz verbosidade dos logs nativos do Spark (Log4j) no console
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session criada com sucesso.")
    return spark


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def extract(spark: SparkSession, input_path: str) -> DataFrame:
    logger.info("Iniciando extração do arquivo: %s", input_path)
    try:
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(input_path)
        )
        total_rows = df.count()
        logger.info("Extração concluída. %s registros lidos.", total_rows)
        return df
    except Exception:
        logger.exception("Falha ao extrair dados do arquivo: %s", input_path)
        raise


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
def parse_multi_format_date(col_name: str):
    """
    Tenta converter a coluna de data testando múltiplos formatos conhecidos
    na base (dd/MM/yyyy e yyyy/MM/dd). Se nenhum formato bater, retorna null
    em vez de estourar exceção (comportamento do try_to_date).
    """
    return F.coalesce(
        F.expr(f"try_to_date({col_name}, 'dd/MM/yyyy')"),
        F.expr(f"try_to_date({col_name}, 'yyyy/MM/dd')"),
        F.expr(f"try_to_date({col_name}, 'yyyy-MM-dd')"),
    )


def clean_monetary_value(col_name: str):
    """
    Limpa valores monetários no formato brasileiro (ex: 'R$ 44.317,88')
    e converte para double de forma segura:
      1. Remove 'R$', espaços e qualquer caractere que não seja dígito,
         ponto, vírgula ou sinal de menos.
      2. Remove o '.' usado como separador de milhar.
      3. Substitui a ',' (separador decimal BR) por '.'.
      4. Usa try_cast para retornar null em vez de estourar exceção
         caso ainda sobre algum valor malformado (em vez de derrubar o job).
    """
    return F.expr(f"""
        try_cast(
            regexp_replace(
                regexp_replace(
                    regexp_replace({col_name}, '[^0-9,.\\\\-]', ''),
                    '\\\\.', ''
                ),
                ',', '.'
            ) as double
        )
        """)


def rename_columns(df: DataFrame) -> DataFrame:
    logger.info("Renomeando colunas...")
    return df.withColumnsRenamed(
        {
            "DT_NASC*": "Data_Nascimento",
            "CNL-ENTRADA": "Canal_Entrada",
            "Vlr$Solicitado": "Valor_Solicitado",
            "RENDA_MENSAL": "Renda_Mensal",
            "dt_solic": "Data_solicitacao",
            "ID_REQ_": "Id_Requisicao",
            "tipoCRED": "Tipo_Credito",
            "PROPOSTA_PARCELA": "Proposta_Parcela",
        }
    )


def treat_date_and_monetary_fields(df: DataFrame) -> DataFrame:
    logger.info("Tratando campos de data e valores monetários...")
    return (
        df.withColumn("Data_solicitacao", parse_multi_format_date("Data_solicitacao"))
        .withColumn("Data_Nascimento", parse_multi_format_date("Data_Nascimento"))
        .withColumn("Valor_Solicitado", clean_monetary_value("Valor_Solicitado"))
        .withColumn("Renda_Mensal", F.expr("try_cast(Renda_Mensal as double)"))
        .withColumn("Proposta_Parcela", F.expr("try_cast(Proposta_Parcela as double)"))
    )


def replace_and_fill_values(df: DataFrame, credit_window: Window) -> DataFrame:
    logger.info("Substituindo valores inválidos e preenchendo nulos...")
    median_group = F.percentile_approx("Renda_Mensal", 0.5)

    return (
        df.withColumn(
            "Score_Pontos",
            F.when(F.col("Score_Pontos") == "ERR", "Nao definido").otherwise(
                F.col("Score_Pontos")
            ),
        )
        .fillna(
            {
                "Canal_Entrada": "Nao Informado",
                "Tipo_Credito": "Nao definido",
                "Proposta_Parcela": 0.0,
                "Score_Pontos": "Nao definido",
            }
        )
        .withColumn(
            "Renda_Mensal",
            F.when(
                F.col("Renda_Mensal").isNull(), median_group.over(credit_window)
            ).otherwise(F.col("Renda_Mensal")),
        )
    )


def drop_invalid_base_records(df: DataFrame) -> DataFrame:
    logger.info("Removendo registros com nulos em colunas obrigatórias...")
    before = df.count()
    df_clean = df.dropna(
        subset=["Id_Requisicao", "Valor_Solicitado", "Data_solicitacao"]
    )
    after = df_clean.count()
    logger.info(
        "Registros removidos por nulos em colunas base: %s (de %s para %s)",
        before - after,
        before,
        after,
    )
    return df_clean


def add_derived_columns(
    df: DataFrame, credit_window: Window, canal_window: Window
) -> DataFrame:
    logger.info("Criando colunas derivadas...")
    return (
        df.withColumn(
            "Media_Renda_Por_Produto",
            F.round(F.mean("Renda_Mensal").over(credit_window), 2),
        )
        .withColumn(
            "Media_Valor_Solicitado_Canal",
            F.round(F.avg("Valor_Solicitado").over(canal_window), 2),
        )
        .withColumn(
            "Idade_Na_Solicitacao",
            F.floor(
                F.months_between(F.col("Data_solicitacao"), F.col("Data_Nascimento"))
                / 12
            ),
        )
        .withColumn(
            "Porcentagem_Renda_Comprometida",
            F.when(
                F.col("Renda_Mensal") > 0,
                F.round(
                    (F.col("Proposta_Parcela") / F.col("Renda_Mensal")) * 100,
                    2,
                ),
            ).otherwise(0.0),
        )
        .withColumn(
            "Risco_Inadimplencia",
            F.when(
                F.col("Porcentagem_Renda_Comprometida") <= 30,
                "Baixo risco - Crédito pode ser aprovado.",
            )
            .when(
                F.col("Porcentagem_Renda_Comprometida") <= 50,
                "Médio risco - Exige análise adicional.",
            )
            .otherwise(
                "Alto risco - Recomenda-se reprovação ou redução do valor solicitado."
            ),
        )
        .withColumn("Ano_Solicitacao", F.year(F.col("Data_solicitacao")))
        .withColumn("Mes_Solicitacao", F.month(F.col("Data_solicitacao")))
    )


def transform(df: DataFrame) -> DataFrame:
    logger.info("Iniciando etapa de transformação...")
    try:
        canal_window = Window.partitionBy("Canal_Entrada")
        credit_window = Window.partitionBy("Tipo_Credito")

        df = rename_columns(df)
        df = treat_date_and_monetary_fields(df)
        df = replace_and_fill_values(df, credit_window)
        df = drop_invalid_base_records(df)
        df = add_derived_columns(df, credit_window, canal_window)

        logger.info("Transformação concluída com sucesso.")
        return df
    except Exception:
        logger.exception("Falha durante a etapa de transformação.")
        raise


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def write_partitioned_csv(df: DataFrame, output_path: str) -> None:
    """
    Escreve os dados particionados por Ano_Solicitacao/Mes_Solicitacao,
    gerando a estrutura de pastas:
      output_path/Ano_Solicitacao=YYYY/Mes_Solicitacao=MM/part-*.csv
    """
    logger.info("Gravando dados particionados por ano/mes em: %s", output_path)
    (
        df.write.mode("overwrite")
        .option("header", "true")
        .partitionBy("Ano_Solicitacao", "Mes_Solicitacao")
        .csv(output_path)
    )
    logger.info("Gravação particionada concluída em: %s", output_path)


def write_consolidated_csv(
    df: DataFrame, output_path: str, file_name: str = "resultado_completo.csv"
) -> None:
    """
    Escreve um único arquivo .csv consolidado, contendo todos os anos e
    meses juntos, na raiz de output_path (mesmo diretório onde ficam as
    pastas Ano_Solicitacao=.../Mes_Solicitacao=...).

    O Spark sempre escreve DataFrames como uma pasta com arquivos part-*,
    então gravamos em uma pasta temporária, movemos o único part-file
    gerado para output_path/file_name e removemos a pasta temporária.

    Observação: essa abordagem assume filesystem local (não HDFS/S3).
    Para object storage/HDFS seria necessário usar a Hadoop FileSystem API
    do Spark em vez de shutil/os.
    """
    tmp_path = os.path.join(output_path, "_tmp_consolidado")
    final_path = os.path.join(output_path, file_name)

    logger.info("Gravando CSV consolidado (todos os anos/meses) em: %s", final_path)

    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_path)

    part_files = glob.glob(os.path.join(tmp_path, "part-*.csv"))
    if not part_files:
        raise FileNotFoundError(
            f"Nenhum arquivo part-*.csv encontrado em {tmp_path} após a escrita."
        )

    if os.path.exists(final_path):
        os.remove(final_path)

    shutil.move(part_files[0], final_path)
    shutil.rmtree(tmp_path)

    logger.info("CSV consolidado gravado com sucesso em: %s", final_path)


def load(df: DataFrame, output_path: str) -> None:
    logger.info("Iniciando carga dos dados em: %s", output_path)
    try:
        write_partitioned_csv(df, output_path)
        write_consolidated_csv(df, output_path)
        logger.info("Carga concluída com sucesso em: %s", output_path)
    except Exception:
        logger.exception("Falha ao gravar os dados em: %s", output_path)
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    input_path = "usuarios_brutos.csv"
    output_path = "resultado_processado"

    spark = None
    try:
        spark = create_spark_session()

        df_raw = extract(spark, input_path)
        df_transformed = transform(df_raw)
        load(df_transformed, output_path)

        logger.info("Pipeline ETL finalizado com sucesso.")
    except Exception:
        logger.exception("Pipeline ETL finalizado com erro.")
        sys.exit(1)
    finally:
        if spark is not None:
            logger.info("Encerrando Spark Session.")
            spark.stop()


if __name__ == "__main__":
    main()
