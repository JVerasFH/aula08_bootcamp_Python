from etl import pipeline_calcular_vendas_consolidado

pasta: str = "data"
formato_de_saida: list = ["csv"]

pipeline_calcular_vendas_consolidado(pasta, formato_de_saida)