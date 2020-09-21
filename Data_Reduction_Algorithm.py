# Databricks notebook source
# MAGIC %run "./Limpieza_datos"

# COMMAND ----------

import pyspark
import pandas as pd
import pyspark.sql.functions as F 
from pyspark.sql.window import Window
from pyspark.sql.functions import lit

# COMMAND ----------

data_filter = data_end.filter(data_end['sector_id'] == 6484) #'\'CLL02489_7B_1\'')

# COMMAND ----------

 if data_filter.select('sector_id').dtypes[0][1] == 'bigint':
    print('funsiona')
    


# COMMAND ----------

data_alg = data_filter.select(data_filter.date_time,data_filter.sector_id, data_filter.accessibility.between(99,100))

# COMMAND ----------

data_alg = data_filter.select('date_time', 'sector_id', 'accessibility').withColumn('condition', data_filter['accessibility'].between(95,100))

# COMMAND ----------

data_renamed = data_alg.withColumnRenamed(str(data_alg.columns[2]), 'condition')

# COMMAND ----------

h = data_renamed.withColumn('numero', F.when(data_renamed['condition'] == True, 1).otherwise(-1))
#Guardamos una columna con el valor numerico de True y False     
df_lag = h.withColumn('estado_anterior',
                        F.lag(h['numero'])
                                 .over(Window.orderBy('sector_id')))
#Guardamos el valor anterior al valor de observación, 
#para saber su estado anterior
result = df_lag.withColumn('derivada',
          (df_lag['numero'] - df_lag['estado_anterior']))
#Calculamos la derivada con la resta del valor actual
#y del estado anterior
g = result.withColumn('Start', F.when(result['derivada'] == -2,result.date_time ))
#El comienzo de la degradación es donde la derivada
#tiene cierto valor negativo, guardandose el date time
s = g.withColumn('End', F.when(g['derivada'] == 2,result.date_time ))
#El fin de la degradación es donde la derivada
#tiene cierto valor positivo, guardandose el date time

# COMMAND ----------

PERIODO = s.select(s.Start, s.End)
PERIODO = PERIODO.dropna(how = 'all')
comienzo = PERIODO.select(PERIODO.Start)
fin = PERIODO.select(PERIODO.End)
Start = comienzo.dropna(how = 'any')
End = fin.dropna(how = 'any')

# COMMAND ----------

data_filter.columns

# COMMAND ----------

def function_1(table, KPI):
  return table.select('date_time', 'sector_id', KPI).withColumn('condition', table[KPI].between(95,100))

# COMMAND ----------

#display(data_filter.select('date_time').unionAll(data_filter.sector_id)
h = data_filter.select('retainability')
type(h)
k = data_filter.select('accessibility')
j = 
display(j)


# COMMAND ----------

def data_reduction(table, limite_superior, limite_inferior, KPI):
  data_alg = table.select('date_time', 'sector_id', KPI).withColumn('condition', table[KPI].between(95,100))
  h = data_alg.withColumn('numero', F.when(data_alg['condition'] == True, 1).otherwise(-1))
#Guardamos una columna con el valor numerico de True y False     
  df_lag = h.withColumn('estado_anterior',
                        F.lag(h['numero'])
                                 .over(Window.orderBy('sector_id')))
#Guardamos el valor anterior al valor de observación, 
#para saber su estado anterior
  result = df_lag.withColumn('derivada',
          (df_lag['numero'] - df_lag['estado_anterior']))
#Calculamos la derivada con la resta del valor actual
#y del estado anterior
  g = result.withColumn('Start', F.when(result['derivada'] == -2,result.date_time ))
#El comienzo de la degradación es donde la derivada
#tiene cierto valor negativo, guardandose el date time
  s = g.withColumn('End', F.when(g['derivada'] == 2,result.date_time ))
#El fin de la degradación es donde la derivada
#tiene cierto valor positivo, guardandose el date time
  PERIODO = s.select(s.Start, s.End).dropna(how = 'all')
  Start = PERIODO.select(PERIODO.Start).dropna(how = 'any')
  End = PERIODO.select(PERIODO.End).dropna(how = 'any')
 
  final = Start.withColumn('End', End.End)
 # final = Start.withColumn()
  #final = [Start.toPandas(), End.toPandas()]
  #resultado=  F.concat(Start, End)
  #Result = pd.concat(final, axis = 1)
  #print(Result)
  display(final)
  
  
  
  

# COMMAND ----------

final = [Start, End]

# COMMAND ----------


def APLICABLE(table, limite_superior, limite_inferior, KPI):
      if data_filter.select('sector_id').dtypes[0][1] == 'bigint':
          print('NOPE')

      else:
        

# COMMAND ----------

for i in data_filter.columns:
  data_reduction(data_filter, 100, 95, i)
  
  

# COMMAND ----------

display(Result)
