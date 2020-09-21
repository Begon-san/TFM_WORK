# Databricks notebook source
#%run "./Limpieza_datos_correlation"


# COMMAND ----------

import numpy as np
import pandas as pd
import glob
import datetime

dateparse = lambda x: pd.datetime.strptime(x.replace("'",''), '%Y-%m-%d %H:%M:%S')
#dateparse2 = lambda x: (x-datetime.datetime(1970,1,1)).total_seconds()
#interpolation = lambda x: x.replace('' , 0)

#pd.set_option('display.max_columns',50)

#path = r"dbfs:/FileStore/tables" # use your path
#all_files = glob.glob(path + "/*.csv")
path = r'/dbfs/FileStore/tables/v3' # use your path
all_files = glob.glob(path + '/*.csv')

pk = [pd.read_csv(f,  parse_dates=['date_time'], date_parser=dateparse, sep = ',', engine = 'python') for f in all_files]

data = pd.concat(pk,ignore_index=True)

#ILOC PARA DATAFRAME, ACCEDER A FILA O COLUMNA
#path = r"dbfs:/FileStore/tables" # use your path
#all_files = glob.glob(path + "/*.csv")
#for i in range(len(pk)):
#data.date_time = data.date_time.apply(dateparse2)
#data = data.set_index('date_time')
   #How es un parametro de dropna que nos permite eliminar o cualquier columna/fila con un valor nan o solo columnas/filas
   #donde todos sus valores sean nan.
data = data.dropna(axis = 1,how = 'all')
   #Ahora rellenamos con 0, estos valores cero nos indican que ha habido un error de calculo si los valores modales se alejan en
   #demasia de este valor por defecto que hemos indexado para rellenar aquellos puntos que no disponian de datos,
   #Si los valores son proximos, podemos tomarlos como una interpolación  lineal que nos ayude a saber, pasando un deterninado thresshold
   #cuando existe un error. Al final el valor cero nos indica si existe un error.
data = data.fillna(0)


# COMMAND ----------

data_1 = spark.createDataFrame(data)

# COMMAND ----------

data_filter = data_1.filter(data_1['sector_id'] == 6484) #'\'CLL02489_7B_1\'')

# COMMAND ----------

import pyspark
import pandas as pd
import pyspark.sql.functions as F 
from pyspark.sql.functions import lit
import pyspark.mllib.stat as S


# COMMAND ----------

from pyspark.sql.window import Window
days = lambda i: (i) * 86400

# COMMAND ----------


#una correlación con la libreria ml, podría ser la solución que estamos buscando ?¿?
#intentemoslo con el mismo bucle, a ver que pasa.
#from pyspark.mllib.stat import Statistics
#from pyspark.ml.stat import Correlation
w = Window.orderBy('date_time').rangeBetween(-days(1), days(0))

# COMMAND ----------

def correlacion(dataset):
  data_a = dataset
  data_b = dataset
  for i in data_a.columns:
    k = data_a.columns.index(i)
    while k  < len(data_a.columns):
      data_b = data_b.withColumn('correlacion_temporal_'+ str(i)+'_'+str(data_a.columns[k]), F.corr(i,data_a.columns[k]).over(w))
      k = k + 1

  return data_b   
      

#for(int i=0; i<length_filas; i++){
 #    for(int j=i; j<length_columnas; j++){
  #        //hacer cosas que tú quieras
   #  }
#}        
 #   for k in data.columns:
#   for i in range(len(data.columns)):
#      df2 = df2.withColumn('correlacion_temporal_'+ k,corr(k,data.columns[i]).over(w))

# COMMAND ----------

from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# UDF for converting column type from vector to double type
unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())

# Iterating over columns to be scaled
# VectorAssembler Transformation - Converting column to vector type
assembler = VectorAssembler(inputCols=['pmztemporary11'],outputCol='pmztemporary11'+"_Vect")

    # MinMaxScaler Transformation
scaler = MinMaxScaler(inputCol='pmztemporary11'+"_Vect", outputCol='pmztemporary11'+"_Scaled")

    # Pipeline of VectorAssembler and MinMaxScaler
pipeline = Pipeline(stages=[assembler, scaler])

    # Fitting pipeline on dataframe
datos_escalados = pipeline.fit(data_filter).transform(data_filter).withColumn('pmztemporary11'+"_Scaled", unlist('pmztemporary11'+"_Scaled")).drop('pmztemporary11'+"_Vect")




# COMMAND ----------

datitos = datos_escalados.withColumn('pmztemporary11_Scaled', datos_escalados.pmztemporary11_Scaled * 100)

# COMMAND ----------

data_filter_2 = data_filter.withColumn('date_time',data_filter.date_time.cast("long"))

# COMMAND ----------

resultado = correlacion(data_filter_2.select('accessibility', 'date_time', 'pmztemporary11', 'retainability'))
resultado = resultado.fillna(0)

# COMMAND ----------

resultado = resultado.withColumn('date_time',F.to_timestamp('date_time'))


# COMMAND ----------

panda_resultado = resultado.toPandas()

# COMMAND ----------

panda_resultado.pmztemporary11

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

def make_patch_spines_invisible(ax):
    ax.set_frame_on(True)
    ax.patch.set_visible(False)
    for sp in ax.spines.values():
        sp.set_visible(False)


fig, ax1 = plt.subplots(figsize=(15, 8))

color = 'tab:red'
ax1.set_xlabel('time (days)')
ax1.set_ylabel('retainability %', color=color)
ax1.plot(panda_resultado.date_time, panda_resultado.retainability, color=color)
ax1.tick_params(axis='y', labelcolor=color)


ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

color_2 = 'tab:blue'
ax2.set_xlabel('time (days)')
ax2.set_ylabel('corr_retnbity_pmZtempo', color=color_2)  # we already handled the x-label with ax1
ax2.plot(panda_resultado.date_time, panda_resultado.correlacion_temporal_pmztemporary11_retainability, color=color_2)
ax2.tick_params(axis='y', labelcolor=color_2)


ax3 = ax1.twinx()  # instantiate a third axes that shares the same x-axis

# Offset the right spine of par2.  The ticks and label have already been
# placed on the right by twinx above.
ax3.spines["right"].set_position(("axes", 1.1))
# Having been created by twinx, par2 has its frame off, so the line of its
# detached spine is invisible.  First, activate the frame but make the patch
# and spines invisible.
make_patch_spines_invisible(ax3)
# Second, show the right spine.
ax3.spines["right"].set_visible(True)

color_3 = 'tab:green'
ax3.set_xlabel('time (days)')
ax3.set_ylabel('pmZtemporary11', color=color_3)  # we already handled the x-label with ax1
ax3.plot(panda_resultado.date_time, panda_resultado.pmztemporary11, color=color_3)
ax3.tick_params(axis='y', labelcolor=color_3)


fig.tight_layout()  # otherwise the right y-label is slightly clipped
ax1.legend(loc = 0)
ax2.legend(loc = 2)
ax3.legend(loc = 9)
plt.show()
