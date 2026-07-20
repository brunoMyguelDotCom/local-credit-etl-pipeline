# ETL de Dados de Usuários com PySpark

> Pipeline de Engenharia de Dados responsável por extrair, transformar e carregar dados brutos de solicitações de crédito, aplicando regras de negócio, enriquecimento de dados, particionamento e geração de arquivos analíticos.

---

## Sumário

1. [Sobre o Projeto](#sobre-o-projeto)
2. [Objetivo](#objetivo)
3. [Arquitetura do Pipeline](#arquitetura-do-pipeline)
4. [Fluxo de Execução (Extract → Transform → Load)](#fluxo-de-execução-extract--transform--load)
5. [Estrutura do Projeto](#estrutura-do-projeto)
6. [Tecnologias Utilizadas](#tecnologias-utilizadas)
7. [Pré-requisitos](#pré-requisitos)
8. [Instalação](#instalação)
9. [Como Executar](#como-executar)
10. [Formato dos Dados de Entrada](#formato-dos-dados-de-entrada)
11. [Formato dos Dados de Saída](#formato-dos-dados-de-saída)
12. [Etapas da Transformação](#etapas-da-transformação)
13. [Regras de Negócio](#regras-de-negócio)
14. [Validações e Tratamento de Dados](#validações-e-tratamento-de-dados)
15. [Estratégia de Particionamento dos Dados](#estratégia-de-particionamento-dos-dados)
16. [Tratamento de Erros](#tratamento-de-erros)
17. [Sistema de Logs](#sistema-de-logs)
18. [Performance e Otimizações Adotadas](#performance-e-otimizações-adotadas)
19. [Possíveis Melhorias / Próximos Passos](#possíveis-melhorias--próximos-passos)
20. [Licença](#licença)
21. [Autor](#autor)
22. [Anexo: Código-Fonte Comentado Linha a Linha](#anexo-código-fonte-comentado-linha-a-linha)

---

## Sobre o Projeto

Este projeto implementa um pipeline **ETL (Extract, Transform, Load)** completo utilizando **Apache Spark** com a API **PySpark** em Python. O pipeline lê um arquivo CSV bruto contendo dados de solicitações de crédito de usuários, aplica um conjunto robusto de transformações (limpeza, padronização, tipagem, derivação de colunas, regras de risco) e grava o resultado em dois formatos:

- **CSV particionado** por `Ano_Solicitacao` / `Mes_Solicitacao` (otimizado para consultas em ferramentas analíticas).
- **CSV consolidado** em um único arquivo, útil para visualização rápida ou distribuição.

O projeto foi desenhado com foco em **boas práticas de Engenharia de Dados**: idempotência, logging estruturado, tratamento de exceções, particionamento, funções de janela e código modular separado por etapas.

---

## Objetivo

Processar uma base bruta (`usuarios_brutos.csv`) com dados de crédito e produzir uma base analítica confiável, enriquecida e padronizada, que possibilite:

- **Análise de risco de inadimplência** com base na relação entre parcela proposta e renda mensal.
- **Estudo de perfil etário** dos solicitantes.
- **Comparação de renda média e valor solicitado médio** por produto de crédito e por canal de entrada.
- **Consultas eficientes por período** (ano/mês) graças ao particionamento dos dados.

---

## Arquitetura do Pipeline
Arquivo CSV de Entrada<br>
<br>↓<br><br>
Extração dos Dados (Extract)<br>
<br>↓<br><br>
Leitura e Carregamento no Spark<br>
<br>↓<br><br>
Renomeação das Colunas<br>
<br>↓<br><br>
Conversão de Datas e Valores Monetários<br>
<br>↓<br><br>
Tratamento de Valores Nulos e Inválidos<br>
<br>↓<br><br>
Validação dos Registros Obrigatórios<br>
<br>↓<br><br>
Aplicação das Regras de Negócio<br>
<br>↓<br><br>
Criação de Colunas Derivadas<br>
<br>↓<br><br>
Particionamento dos Dados<br>
<br>↓<br><br>
Exportação dos Arquivos Processados (Load)<br>
<br>↓<br><br>
CSV Consolidado + CSVs Particionados<br>
<br>↓<br><br>
Logs de Execução
---

## Fluxo de Execução (Extract → Transform → Load)

### 1. Extract
- Lê o arquivo `usuarios_brutos.csv` a partir do diretório de trabalho.
- Usa `spark.read.format("csv")` com `header=True` e `inferSchema=False` (a tipagem é aplicada manualmente na etapa de Transform).
- Loga o total de registros lidos.

### 2. Transform
Pipeline modular em 6 funções encadeadas:

| # | Função                       | Responsabilidade                                                              |
|---|------------------------------|-------------------------------------------------------------------------------|
| 1 | `rename_columns`             | Renomeia colunas com caracteres especiais para nomes padronizados.            |
| 2 | `treat_date_and_monetary_fields` | Faz o parse de múltiplos formatos de data e limpeza de valores monetários.|
| 3 | `replace_and_fill_values`    | Substitui `"ERR"`, preenche nulos e imputa `Renda_Mensal` com mediana.        |
| 4 | `drop_invalid_base_records`  | Remove registros sem `Id_Requisicao`, `Valor_Solicitado` ou `Data_solicitacao`. |
| 5 | `add_derived_columns`        | Cria colunas analíticas via Window Functions e classifica risco.              |
| 6 | `transform`                  | Orquestra todas as funções acima.                                             |

### 3. Load
- **`write_partitioned_csv`** → grava os dados particionados por `Ano_Solicitacao` / `Mes_Solicitacao`.
- **`write_consolidated_csv`** → grava um único arquivo `resultado_completo.csv` usando `coalesce(1)`.

---

## Estrutura do Projeto

```
etl-pyspark/
│
├── main.py                  # Pipeline ETL completo (Entry point)
├── README.md                # Este arquivo
├── etl_user_data.log        # Arquivo de log gerado em tempo de execução
│
├── usuarios_brutos.csv      # (Entrada) Dados brutos a serem processados
│
└── resultado_processado/    # (Saída) Diretório gerado após execução
    ├── Ano_Solicitacao=2024/
    │   ├── Mes_Solicitacao=1/
    │   │   └── part-00000-*.csv
    │   ├── Mes_Solicitacao=2/
    │   │   └── part-00000-*.csv
    │   └── ...
    ├── Ano_Solicitacao=2025/
    │   └── ...
    └── resultado_completo.csv
```

---

## Tecnologias Utilizadas

| Tecnologia          | Versão Recomendada | Uso                                                       |
|---------------------|--------------------|-----------------------------------------------------------|
| **Python**          | 3.9+               | Linguagem principal.                                      |
| **Apache Spark**    | 3.4+               | Motor de processamento distribuído.                       |
| **PySpark**         | 3.4+               | API Python para Spark.                                    |
| **Java (JDK)**      | 8 / 11             | Dependência obrigatória do Spark.                         |
| **Logging (stdlib)**| nativo             | Sistema de logs estruturado em arquivo e console.         |
| **Glob / Shutil**   | nativo             | Manipulação de arquivos e diretórios.                     |

---

## Pré-requisitos

- **Python 3.9 ou superior**
- **Java JDK 8 ou 11** instalado e `JAVA_HOME` configurado
- **Apache Spark 3.4+** (ou pacote `pyspark` instalado via `pip`)
- Mínimo de **4 GB de RAM** disponível (configurado `spark.driver.memory=4g`)
- 2 cores lógicos disponíveis (configurado `local[2]`)

Verifique seu ambiente:

```bash
python --version
java -version
echo $JAVA_HOME
```

---

## Instalação

### Opção 1: Instalação local do PySpark (recomendada para iniciantes)

```bash
# Crie e ative um ambiente virtual
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
# .venv\Scripts\activate         # Windows

# Instale o PySpark
pip install pyspark
```

### Opção 2: Spark standalone

1. Baixe o Spark em [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)
2. Extraia em um diretório de sua preferência
3. Configure `SPARK_HOME` e adicione `$SPARK_HOME/bin` ao `PATH`
4. Exporte `PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH`

---

## Como Executar

1. **Posicione o arquivo de entrada** no mesmo diretório do `main.py`:

```bash
cp caminho/para/usuarios_brutos.csv .
```

2. **Execute o pipeline**:

```bash
python main.py
```

3. **Verifique os resultados** em:

```
./resultado_processado/
```

4. **Consulte os logs** em:

```
./etl_user_data.log
```

---

## Formato dos Dados de Entrada

Arquivo: **`usuarios_brutos.csv`** (CSV com cabeçalho, separador `,`).

### Colunas originais (com nomes possivelmente "sujos"):

| Coluna Original    | Tipo (origem) | Descrição                                      |
|--------------------|---------------|------------------------------------------------|
| `ID_REQ_`          | string        | Identificador único da requisição de crédito.  |
| `dt_solic`         | string        | Data da solicitação.                           |
| `DT_NASC*`         | string        | Data de nascimento do solicitante.             |
| `CNL-ENTRADA`      | string        | Canal pelo qual o cliente entrou.              |
| `tipoCRED`         | string        | Tipo de produto de crédito solicitado.        |
| `Vlr$Solicitado`   | string        | Valor solicitado (formato BR: `R$ 1.234,56`).  |
| `RENDA_MENSAL`     | string        | Renda mensal do solicitante.                   |
| `PROPOSTA_PARCELA` | string        | Valor da parcela proposta.                     |
| `Score_Pontos`     | string        | Score de crédito (pode conter `"ERR"`).        |

---

## Formato dos Dados de Saída

### 1. CSV Particionado

Estrutura de diretórios:

```
resultado_processado/
├── Ano_Solicitacao=2024/
│   ├── Mes_Solicitacao=1/
│   │   └── part-00000-<uuid>.csv
│   ├── Mes_Solicitacao=2/
│   │   └── part-00000-<uuid>.csv
│   └── ...
└── Ano_Solicitacao=2025/
    └── ...
```

Cada arquivo `part-*.csv` contém as colunas abaixo:

| Coluna                            | Tipo      | Descrição                                              |
|-----------------------------------|-----------|--------------------------------------------------------|
| `Id_Requisicao`                   | string    | ID único da requisição.                                |
| `Data_solicitacao`                | date      | Data da solicitação (parseada).                        |
| `Data_Nascimento`                 | date      | Data de nascimento (parseada).                         |
| `Canal_Entrada`                   | string    | Canal de entrada (nulos → `"Nao Informado"`).          |
| `Tipo_Credito`                    | string    | Tipo de crédito (nulos → `"Nao definido"`).            |
| `Valor_Solicitado`                | double    | Valor solicitado (limpo).                              |
| `Renda_Mensal`                    | double    | Renda mensal (nulos preenchidos com mediana).          |
| `Proposta_Parcela`                | double    | Valor da parcela (nulos → `0.0`).                      |
| `Score_Pontos`                    | string    | Score (`"ERR"` ou nulos → `"Nao definido"`).           |
| `Media_Renda_Por_Produto`         | double    | Média de renda por `Tipo_Credito`.                     |
| `Media_Valor_Solicitado_Canal`    | double    | Média de valor solicitado por `Canal_Entrada`.         |
| `Idade_Na_Solicitacao`            | int       | Idade do solicitante no momento da solicitação.        |
| `Porcentagem_Renda_Comprometida`  | double    | % da renda comprometida com a parcela.                 |
| `Risco_Inadimplencia`             | string    | Classificação de risco (ver Regras de Negócio).        |
| `Ano_Solicitacao`                 | int       | Ano (partição).                                        |
| `Mes_Solicitacao`                 | int       | Mês (partição).                                        |

### 2. CSV Consolidado

Arquivo único na raiz de `resultado_processado/`:

```
resultado_processado/resultado_completo.csv
```

Contém **todos os registros processados**, com as mesmas colunas do CSV particionado, **sem** a estrutura de pastas (apenas as colunas `Ano_Solicitacao` e `Mes_Solicitacao` permanecem como colunas normais).

---

## Etapas da Transformação

```
DataFrame bruto
    │
    ├─▶ rename_columns()
    │     • Renomeia: DT_NASC* → Data_Nascimento
    │     •          CNL-ENTRADA → Canal_Entrada
    │     •          Vlr$Solicitado → Valor_Solicitado
    │     •          RENDA_MENSAL → Renda_Mensal
    │     •          dt_solic → Data_solicitacao
    │     •          ID_REQ_ → Id_Requisicao
    │     •          tipoCRED → Tipo_Credito
    │     •          PROPOSTA_PARCELA → Proposta_Parcela
    │
    ├─▶ treat_date_and_monetary_fields()
    │     • Datas: try_to_date em 3 formatos (dd/MM/yyyy, yyyy/MM/dd, yyyy-MM-dd)
    │     • Valor_Solicitado: remove "R$", separador de milhar e converte vírgula
    │     • Renda_Mensal e Proposta_Parcela: try_cast → double
    │
    ├─▶ replace_and_fill_values()
    │     • Score_Pontos == "ERR" → "Nao definido"
    │     • fillna em Canal_Entrada, Tipo_Credito, Proposta_Parcela, Score_Pontos
    │     • Renda_Mensal nula → mediana por Tipo_Credito
    │
    ├─▶ drop_invalid_base_records()
    │     • Remove linhas sem Id_Requisicao, Valor_Solicitado, Data_solicitacao
    │
    └─▶ add_derived_columns()
          • Media_Renda_Por_Produto (Window por Tipo_Credito)
          • Media_Valor_Solicitado_Canal (Window por Canal_Entrada)
          • Idade_Na_Solicitacao (months_between / 12, floor)
          • Porcentagem_Renda_Comprometida
          • Risco_Inadimplencia (when aninhado)
          • Ano_Solicitacao, Mes_Solicitacao
```

---

## Regras de Negócio

### Classificação de Risco de Inadimplência

A coluna **`Porcentagem_Renda_Comprometida`** é calculada como:

```
Porcentagem_Renda_Comprometida = (Proposta_Parcela / Renda_Mensal) * 100
```

Com base nesse percentual, a coluna **`Risco_Inadimplencia`** é classificada em:

| Faixa do Percentual | Classificação                                | Mensagem                                              |
|---------------------|----------------------------------------------|-------------------------------------------------------|
| `<= 30%`            | Baixo risco                                  | `"Baixo risco - Crédito pode ser aprovado."`         |
| `> 30%` e `<= 50%`  | Médio risco                                  | `"Médio risco - Exige análise adicional."`           |
| `> 50%`             | Alto risco                                   | `"Alto risco - Recomenda-se reprovação ou redução do valor solicitado."` |

> **Observação:** quando `Renda_Mensal <= 0`, a `Porcentagem_Renda_Comprometida` é definida como `0.0` para evitar divisão por zero.

---

## Validações e Tratamento de Dados

| Validação                                      | Tratamento                                                       |
|------------------------------------------------|------------------------------------------------------------------|
| Datas em formatos diversos                     | `try_to_date` com `coalesce` em 3 formatos; se nenhum bate, retorna `null`. |
| Valor monetário com `R$`, `.` e `,`            | Regex remove caracteres inválidos, separador de milhar e troca `,` por `.`; depois `try_cast as double`. |
| `Score_Pontos == "ERR"`                        | Substituído por `"Nao definido"`.                                |
| `Canal_Entrada` nulo                           | Preenchido com `"Nao Informado"`.                                |
| `Tipo_Credito` nulo                            | Preenchido com `"Nao definido"`.                                 |
| `Proposta_Parcela` nula                        | Preenchida com `0.0`.                                            |
| `Score_Pontos` nulo                            | Preenchido com `"Nao definido"`.                                 |
| `Renda_Mensal` nula                            | Imputada com a **mediana** (`percentile_approx(0.5)`) por `Tipo_Credito`. |
| `Id_Requisicao`, `Valor_Solicitado` ou `Data_solicitacao` nulos | Linha **descartada** com log de qtd. removida.    |
| Valores impossíveis de cast                    | `try_cast` → `null` em vez de derrubar o job.                    |

---

## Estratégia de Particionamento dos Dados

- **Colunas de partição:** `Ano_Solicitacao` e `Mes_Solicitacao` (nessa ordem).
- **Formato no filesystem:**

  ```
  output_path/Ano_Solicitacao=YYYY/Mes_Solicitacao=MM/part-*.csv
  ```

- **Vantagens:**
  - **Pruning de partição** em queries SQL: filtros por ano/mês leem apenas os diretórios relevantes.
  - Compatibilidade com **Hive-style partitioning** (consumível por Athena, BigQuery, Presto, Hive, DuckDB etc.).
  - **Escalabilidade** — facilita mover para S3/HDFS sem alterar o código de leitura downstream.
- **Decisão do ano/mês:** o particionamento é feito **após** o parsing de `Data_solicitacao`, garantindo que apenas registros com data válida sejam particionados (linhas sem data válida já são descartadas em `drop_invalid_base_records`).

---

## Tratamento de Erros

- **Try/except em todas as etapas críticas** (`extract`, `transform`, `load`, `main`).
- **Re-raise explícito** após log da exceção: o erro é registrado e propagado para a função chamadora.
- **`logger.exception`** é usado em todos os blocos de captura — ele já inclui o stack trace completo.
- **Saída controlada**: em `main()`, qualquer exceção resulta em `sys.exit(1)`, sinalizando falha para o sistema operacional e para pipelines de CI/CD.
- **Recursos garantidos via `finally`**: a `SparkSession` é sempre encerrada com `spark.stop()`, evitando processos órfãos.
- **Pastas temporárias**: `write_consolidated_csv` limpa a pasta temporária `_tmp_consolidado` antes e depois da escrita, evitando mistura com execuções anteriores.

---

## Sistema de Logs

- **Configuração centralizada** com `logging.basicConfig` no topo do arquivo.
- **Formato padronizado:**

  ```
  2026-07-20 14:33:12,123 | INFO     | EtlUserData | Mensagem...
  ```

- **Dois handlers:**
  - **`StreamHandler(sys.stdout)`** → console (feedback em tempo real).
  - **`FileHandler("etl_user_data.log", mode="a", encoding="utf-8")`** → arquivo (modo append, preserva histórico entre execuções).
- **Nível:** `INFO` por padrão; logs nativos do Spark (Log4j) são reduzidos para `WARN` via `setLogLevel("WARN")`, diminuindo ruído no console.
- **Logger dedicado:** `logger = logging.getLogger("EtlUserData")` — facilita filtros em produção.
- **Pontos de log relevantes:**
  - Início/fim de cada etapa (extract/transform/load).
  - Quantidade de registros lidos, removidos e processados.
  - Início/fim da Spark Session.
  - Exceções com stack trace completo em caso de falha.

---

## Performance e Otimizações Adotadas

| Otimização                                | Onde é aplicada                                      | Benefício                                                                 |
|-------------------------------------------|------------------------------------------------------|---------------------------------------------------------------------------|
| **`coalesce(1)`**                         | `write_consolidated_csv`                             | Reduz N partições a 1 antes da escrita, gerando um único arquivo CSV.     |
| **`partitionBy` em escrita**              | `write_partitioned_csv`                              | Acelera leituras por ano/mês em engines downstream (pruning).            |
| **Window Functions**                      | `add_derived_columns`                                | Calcula médias por partição lógica (`Tipo_Credito`, `Canal_Entrada`) sem precisar de joins. |
| **Schemas explícitos (sem `inferSchema`)**| `extract`                                            | Evita uma varredura extra do CSV; mais rápido e previsível.              |
| **`try_cast` / `try_to_date`**            | Tratamento de data e monetário                       | Não derruba o job em valores malformados — converte para `null`.          |
| **Mediana via `percentile_approx`**       | Imputação de `Renda_Mensal`                          | Mais robusta à outliers que a média para dados de renda.                  |
| **`setLogLevel("WARN")`**                 | Spark Session                                        | Reduz I/O de log e melhora performance percebida.                         |
| **`spark.sql.shuffle.partitions=4`**      | Spark Session                                        | Adequado para `local[2]` — evita overhead de 200 partições default.      |
| **`spark.driver.memory=4g`**              | Spark Session                                        | Evita `OutOfMemoryError` em bases grandes durante collect/join.           |
| **Modularização em funções**              | Todo o pipeline                                      | Facilita testes unitários, profiling por etapa e reuso.                   |

---

## Autor

**Bruno** — Aspirante a Engenheiro de Dados

---

# Anexo: Código-Fonte Comentado Linha a Linha

> A seguir, o conteúdo completo de `main.py` com **comentários linha a linha** explicando exatamente o que cada parte faz.

```python
# ============================================================================
# IMPORTS
# ============================================================================

# Permite localizar arquivos usando padrões Unix-like (ex.: "part-*.csv").
import glob

# Biblioteca padrão de logging do Python. Usada para registrar eventos
# (info, warning, erro) durante a execução do pipeline.
import logging

# Oferece utilitários para interagir com o sistema operacional
# (criar/remover diretórios, checar existência de paths etc.).
import os

# Fornece operações de alto nível sobre arquivos e diretórios
# (mover, copiar, remover árvores inteiras).
import shutil

# Dá acesso a variáveis e funções do interpretador Python
# (neste projeto, usado para sys.exit(1) em caso de erro fatal).
import sys

# Importa a SparkSession (ponto de entrada para usar Spark) e o tipo
# DataFrame, usado nas anotações de tipo das funções.
from pyspark.sql import SparkSession, DataFrame

# Importa o módulo de funções do PySpark com o alias F.
# É através de F.col, F.when, F.coalesce etc. que construímos a maior
# parte das transformações do pipeline.
from pyspark.sql import functions as F

# Importa Window, que permite definir partições lógicas sobre as quais
# aplicamos funções de agregação "por grupo" (médias, medianas, etc.)
# sem precisar fazer joins ou groupBy.
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# CONFIGURAÇÃO DO SISTEMA DE LOGS
# ---------------------------------------------------------------------------
# basicConfig define o comportamento padrão do módulo logging:
#   - level: severidade mínima das mensagens que serão registradas.
#   - format: layout da mensagem (data | nível | nome-do-logger | msg).
#   - handlers: destinos onde os logs serão enviados.
logging.basicConfig(
    level=logging.INFO,  # INFO, WARNING, ERROR e CRITICAL serão registrados.
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        # Envia logs para a saída padrão (terminal), permitindo acompanhamento
        # em tempo real durante a execução.
        logging.StreamHandler(sys.stdout),

        # Grava logs em arquivo no modo append ("a"), preservando o histórico
        # de execuções anteriores. UTF-8 garante suporte a acentos.
        logging.FileHandler("etl_user_data.log", mode="a", encoding="utf-8"),
    ],
)

# Cria um logger nomeado "EtlUserData". Recomenda-se um logger dedicado
# por pipeline para facilitar filtros em produção.
logger = logging.getLogger("EtlUserData")


# ---------------------------------------------------------------------------
# CRIAÇÃO DA SPARK SESSION
# ---------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    # Loga que o processo de criação está começando.
    logger.info("Criando/recuperando Spark Session...")

    # SparkSession.builder permite configurar a sessão passo a passo.
    spark = (
        SparkSession.builder
        .appName("EtlUserData")            # Nome amigável que aparece na UI do Spark.
        .master("local[2]")                # Executa localmente usando 2 threads.
        .config("spark.driver.memory", "4g")# Memória alocada para o driver (4 GB).
        .config("spark.sql.shuffle.partitions", "4")  # Nº de partições em shuffles (baixo pq é local).
        .getOrCreate()                     # Cria nova sessão ou recupera uma existente.
    )

    # Reduz a verbosidade dos logs nativos do Spark (Log4j) para WARN,
    # diminuindo ruído no console.
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark Session criada com sucesso.")
    return spark


# ---------------------------------------------------------------------------
# ETAPA 1: EXTRACT
# ---------------------------------------------------------------------------
def extract(spark: SparkSession, input_path: str) -> DataFrame:
    # Loga o caminho do arquivo que será lido.
    logger.info("Iniciando extração do arquivo: %s", input_path)
    try:
        # Lê o CSV usando o Spark:
        #   - format("csv"): informa o formato dos dados.
        #   - option("header", "true"): primeira linha contém nomes de colunas.
        #   - option("inferSchema", "false"): NÃO infere o schema automaticamente;
        #     todas as colunas virão como string e a tipagem é feita depois.
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(input_path)
        )

        # Conta o total de registros lidos (operação que materializa o DataFrame).
        total_rows = df.count()

        # Loga a quantidade lida para rastreabilidade.
        logger.info("Extração concluída. %s registros lidos.", total_rows)

        return df
    except Exception:
        # Em caso de erro, registra a exceção completa (com stack trace).
        logger.exception("Falha ao extrair dados do arquivo: %s", input_path)
        # Propaga a exceção para que a função chamadora decida o que fazer.
        raise


# ---------------------------------------------------------------------------
# ETAPA 2: TRANSFORM
# ---------------------------------------------------------------------------

def parse_multi_format_date(col_name: str):
    """
    Recebe o nome de uma coluna e devolve uma expressão Spark que tenta
    converter a coluna para DATE testando múltiplos formatos:
      1) dd/MM/yyyy   (formato brasileiro com barras)
      2) yyyy/MM/dd   (formato ISO-like com barras)
      3) yyyy-MM-dd   (formato ISO com hifens)
    Se nenhum formato casar, retorna null (em vez de erro) graças ao try_to_date.
    O coalesce escolhe a primeira expressão não-nula para cada linha.
    """
    return F.coalesce(
        F.expr(f"try_to_date({col_name}, 'dd/MM/yyyy')"),
        F.expr(f"try_to_date({col_name}, 'yyyy/MM/dd')"),
        F.expr(f"try_to_date({col_name}, 'yyyy-MM-dd')"),
    )


def clean_monetary_value(col_name: str):
    """
    Limpa valores monetários em formato brasileiro (ex.: "R$ 44.317,88")
    e converte para double de forma defensiva:

      1) regexp_replace 1: remove qualquer caractere que NÃO seja dígito,
         ponto, vírgula ou hífen (tira "R$", espaços, etc.).
      2) regexp_replace 2: remove todos os pontos (separadores de milhar).
      3) regexp_replace 3: troca a vírgula decimal por ponto (padrão US/UK).
      4) try_cast: converte para double; se não der, devolve null em vez de erro.
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
    # Loga o início da renomeação.
    logger.info("Renomeando colunas...")

    # withColumnsRenamed aceita um dicionário {nome_antigo: nome_novo}
    # e devolve um DataFrame com as colunas renomeadas.
    return df.withColumnsRenamed(
        {
            # Padroniza nomes "sujos" (com *, $, -, _) para nomes analíticos.
            "DT_NASC*":         "Data_Nascimento",
            "CNL-ENTRADA":      "Canal_Entrada",
            "Vlr$Solicitado":   "Valor_Solicitado",
            "RENDA_MENSAL":     "Renda_Mensal",
            "dt_solic":         "Data_solicitacao",
            "ID_REQ_":          "Id_Requisicao",
            "tipoCRED":         "Tipo_Credito",
            "PROPOSTA_PARCELA": "Proposta_Parcela",
        }
    )


def treat_date_and_monetary_fields(df: DataFrame) -> DataFrame:
    # Loga o início do tratamento.
    logger.info("Tratando campos de data e valores monetários...")

    return (
        # Converte as colunas de data testando múltiplos formatos.
        df.withColumn("Data_solicitacao", parse_multi_format_date("Data_solicitacao"))
          .withColumn("Data_Nascimento",  parse_multi_format_date("Data_Nascimento"))

        # Limpa e converte o valor solicitado (formato BR → double).
          .withColumn("Valor_Solicitado",  clean_monetary_value("Valor_Solicitado"))

        # Cast seguro para double: se houver lixo na coluna, vira null.
          .withColumn("Renda_Mensal",      F.expr("try_cast(Renda_Mensal as double)"))
          .withColumn("Proposta_Parcela",  F.expr("try_cast(Proposta_Parcela as double)"))
    )


def replace_and_fill_values(df: DataFrame, credit_window: Window) -> DataFrame:
    # Loga o início da etapa de limpeza de valores inválidos/nulos.
    logger.info("Substituindo valores inválidos e preenchendo nulos...")

    # Define a expressão de mediana (percentil 50) da coluna Renda_Mensal
    # calculada por partição de Tipo_Credito (usada na imputação).
    median_group = F.percentile_approx("Renda_Mensal", 0.5)

    return (
        # Substitui o valor "ERR" no Score_Pontos por "Nao definido".
        df.withColumn(
            "Score_Pontos",
            F.when(F.col("Score_Pontos") == "ERR", "Nao definido")
             .otherwise(F.col("Score_Pontos")),
        )

        # Preenche nulos em colunas categóricas/numéricas com valores default.
        .fillna(
            {
                "Canal_Entrada":    "Nao Informado",
                "Tipo_Credito":     "Nao definido",
                "Proposta_Parcela": 0.0,
                "Score_Pontos":     "Nao definido",
            }
        )

        # Imputa a Renda_Mensal nula com a mediana do Tipo_Credito.
        # Janela "credit_window" já foi construída em transform() e é passada
        # por parâmetro para evitar recriá-la aqui.
        .withColumn(
            "Renda_Mensal",
            F.when(
                F.col("Renda_Mensal").isNull(),
                median_group.over(credit_window)
            ).otherwise(F.col("Renda_Mensal")),
        )
    )


def drop_invalid_base_records(df: DataFrame) -> DataFrame:
    # Loga que a limpeza de linhas inválidas está começando.
    logger.info("Removendo registros com nulos em colunas obrigatórias...")

    # Conta antes da remoção para o log informativo.
    before = df.count()

    # dropna com subset remove apenas linhas que têm nulo nas colunas
    # listadas. Linhas com nulo em outras colunas são preservadas.
    df_clean = df.dropna(
        subset=["Id_Requisicao", "Valor_Solicitado", "Data_solicitacao"]
    )

    # Conta depois da remoção.
    after = df_clean.count()

    # Loga quantas linhas saíram e o total antes/depois.
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
    # Loga o início da criação de colunas analíticas.
    logger.info("Criando colunas derivadas...")

    return (
        # Média de renda agrupada por Tipo_Credito, arredondada para 2 casas.
        df.withColumn(
            "Media_Renda_Por_Produto",
            F.round(F.mean("Renda_Mensal").over(credit_window), 2),
        )

        # Média de valor solicitado agrupada por Canal_Entrada.
        .withColumn(
            "Media_Valor_Solicitado_Canal",
            F.round(F.avg("Valor_Solicitado").over(canal_window), 2),
        )

        # Idade na solicitação = meses entre nascimento e solicitação / 12,
        # arredondado para baixo (floor) para obter anos inteiros.
        .withColumn(
            "Idade_Na_Solicitacao",
            F.floor(
                F.months_between(F.col("Data_solicitacao"), F.col("Data_Nascimento"))
                / 12
            ),
        )

        # % da renda comprometida com a parcela.
        # Guard contra Renda_Mensal <= 0 para evitar divisão por zero:
        # nesse caso, devolve 0.0.
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

        # Classificação de risco em três faixas, com base no percentual
        # comprometido. É uma cadeia de F.when aninhados:
        #   <=30%  -> Baixo risco
        #   <=50%  -> Médio risco
        #   >50%   -> Alto risco
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

        # Colunas auxiliares para particionamento da escrita.
        .withColumn("Ano_Solicitacao", F.year(F.col("Data_solicitacao")))
        .withColumn("Mes_Solicitacao", F.month(F.col("Data_solicitacao")))
    )


def transform(df: DataFrame) -> DataFrame:
    # Loga o início do pipeline de transformação.
    logger.info("Iniciando etapa de transformação...")
    try:
        # Define as duas janelas usadas nas agregações por grupo.
        canal_window = Window.partitionBy("Canal_Entrada")
        credit_window = Window.partitionBy("Tipo_Credito")

        # Aplica cada etapa em sequência, sempre reatribuindo a df.
        df = rename_columns(df)
        df = treat_date_and_monetary_fields(df)
        df = replace_and_fill_values(df, credit_window)
        df = drop_invalid_base_records(df)
        df = add_derived_columns(df, credit_window, canal_window)

        logger.info("Transformação concluída com sucesso.")
        return df
    except Exception:
        # Qualquer falha é logada com stack trace e propagada.
        logger.exception("Falha durante a etapa de transformação.")
        raise


# ---------------------------------------------------------------------------
# ETAPA 3: LOAD
# ---------------------------------------------------------------------------
def write_partitioned_csv(df: DataFrame, output_path: str) -> None:
    """
    Grava os dados particionados por Ano_Solicitacao/Mes_Solicitacao,
    gerando a estrutura de pastas:
      output_path/Ano_Solicitacao=YYYY/Mes_Solicitacao=MM/part-*.csv
    """
    logger.info("Gravando dados particionados por ano/mes em: %s", output_path)

    # Escreve o DataFrame:
    #   - mode("overwrite"): sobrescreve dados anteriores.
    #   - option("header","true"): inclui a linha de cabeçalho em cada part file.
    #   - partitionBy: cria subdiretórios no formato Hive-style.
    #   - csv: formato CSV.
    (
        df.write
          .mode("overwrite")
          .option("header", "true")
          .partitionBy("Ano_Solicitacao", "Mes_Solicitacao")
          .csv(output_path)
    )

    logger.info("Gravação particionada concluída em: %s", output_path)


def write_consolidated_csv(
    df: DataFrame, output_path: str, file_name: str = "resultado_completo.csv"
) -> None:
    """
    Escreve um único arquivo CSV consolidado (todos os anos/meses juntos)
    na raiz de output_path.

    O Spark sempre grava DataFrames como uma pasta contendo arquivos part-*,
    então a estratégia é:
      1) coalesce(1) força uma única partição.
      2) Escreve em uma pasta temporária.
      3) Localiza o único part-*.csv gerado.
      4) Move esse arquivo para o caminho final com o nome desejado.
      5) Remove a pasta temporária.

    Observação: esta abordagem assume filesystem local (não HDFS/S3).
    Para object storage/HDFS, seria necessário usar a Hadoop FileSystem API
    do Spark em vez de shutil/os.
    """
    # Define o caminho da pasta temporária e do arquivo final.
    tmp_path = os.path.join(output_path, "_tmp_consolidado")
    final_path = os.path.join(output_path, file_name)

    logger.info("Gravando CSV consolidado (todos os anos/meses) em: %s", final_path)

    # Garante que a pasta temporária esteja limpa antes de começar.
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)

    # coalesce(1) reduz as partições a UMA — o que faz o Spark gerar
    # exatamente um arquivo part-*.csv. Sem coalesce, o Spark geraria
    # vários arquivos, e a etapa de "consolidação" não funcionaria.
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_path)

    # Localiza o(s) arquivo(s) part-*.csv gerados dentro da pasta temporária.
    part_files = glob.glob(os.path.join(tmp_path, "part-*.csv"))
    if not part_files:
        # Se não houver nenhum, algo deu errado — falha explicitamente.
        raise FileNotFoundError(
            f"Nenhum arquivo part-*.csv encontrado em {tmp_path} após a escrita."
        )

    # Se já existir um arquivo final de uma execução anterior, remove.
    if os.path.exists(final_path):
        os.remove(final_path)

    # Move o part-file único para o destino final com nome amigável.
    shutil.move(part_files[0], final_path)

    # Limpa a pasta temporária (que já ficou vazia após o move).
    shutil.rmtree(tmp_path)

    logger.info("CSV consolidado gravado com sucesso em: %s", final_path)


def load(df: DataFrame, output_path: str) -> None:
    # Loga o início da etapa de carga.
    logger.info("Iniciando carga dos dados em: %s", output_path)
    try:
        # 1) Grava versão particionada (otimizada para leitura analítica).
        write_partitioned_csv(df, output_path)

        # 2) Grava versão consolidada (um único CSV para visualização rápida).
        write_consolidated_csv(df, output_path)

        logger.info("Carga concluída com sucesso em: %s", output_path)
    except Exception:
        # Loga o erro com stack trace e propaga.
        logger.exception("Falha ao gravar os dados em: %s", output_path)
        raise


# ---------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL (ENTRY POINT)
# ---------------------------------------------------------------------------
def main():
    # Define os caminhos de entrada e saída por padrão.
    # Podem ser facilmente transformados em parâmetros CLI/ambiente.
    input_path = "usuarios_brutos.csv"
    output_path = "resultado_processado"

    spark = None  # Inicializa a variável para que o finally funcione sempre.
    try:
        # Cria/recupera a SparkSession.
        spark = create_spark_session()

        # Executa o pipeline completo: Extract → Transform → Load.
        df_raw = extract(spark, input_path)
        df_transformed = transform(df_raw)
        load(df_transformed, output_path)

        logger.info("Pipeline ETL finalizado com sucesso.")
    except Exception:
        # Em caso de qualquer falha, loga o erro e sai com código 1,
        # sinalizando falha para o sistema operacional e ferramentas de CI/CD.
        logger.exception("Pipeline ETL finalizado com erro.")
        sys.exit(1)
    finally:
        # Garante que a SparkSession será encerrada mesmo em caso de erro,
        # liberando recursos e evitando processos órfãos.
        if spark is not None:
            logger.info("Encerrando Spark Session.")
            spark.stop()


# Bloco padrão Python: só executa main() quando o script for invocado
# diretamente (python main.py), e não quando importado como módulo.
if __name__ == "__main__":
    main()
```

---

<p align="center">
  Feito com muito <code>DataFrame.transform()</code>
</p>
