# Databricks notebook source
# MAGIC %md
# MAGIC ###**Transformando os dados para a camada bronze**
# MAGIC As configurações são:
# MAGIC   - Remover informações não relavantes para a empresa, como imagens ou usuários dos imóveis
# MAGIC   - Garantir a presença de uma coluna de identificação única para cada imóvel utilizando o campo "id"
# MAGIC
# MAGIC **Observação**: Os dados devem ser salvos na camada bronze em formato delta

# COMMAND ----------

display(dbutils.fs.ls("/mnt/dados/inbound"))

# COMMAND ----------

path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
data = spark.read.json(path)
display(data)

# COMMAND ----------

data_mod = data.drop("imagens", "usuario")

# COMMAND ----------

# MAGIC %md
# MAGIC **Criando uma coluna de identificação e salvando na camada bronze**

# COMMAND ----------

from pyspark.sql.functions import col
df_bronze = data_mod.withColumn("id", col("anuncio.id"))
df_bronze.write.format("delta").mode("overwrite").save("/mnt/dados/bronze/dataset_imoveis")
