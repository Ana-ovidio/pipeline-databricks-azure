# Databricks notebook source
# MAGIC %md
# MAGIC ###**Transformando os dados da camada bronze para a camada silver**
# MAGIC Condições: 
# MAGIC   - Converter cada campo do JSON em uma coluna individual
# MAGIC   - Remover coluna(s) que contenham informações sobre as características dos imóveis, pois não são necessárias para esta fase

# COMMAND ----------

dbutils.fs.ls("/mnt/dados/bronze")
df = spark.read.format("delta").load("/mnt/dados/bronze/dataset_imoveis/")
display(df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Transformando o json em colunas

# COMMAND ----------

display(df.select("anuncio.*").head(5))

# COMMAND ----------

df_mod = df.select("anuncio.*", "anuncio.endereco.*")
df_mod = df_mod.drop("endereco", "caracteristicas")
display(df_mod.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Salvando na camada silver**

# COMMAND ----------

df_mod.write.format("delta").mode("overwrite").save("/mnt/dados/silver/dataset_imoveis/")
