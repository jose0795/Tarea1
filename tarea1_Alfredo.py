#TAREA 1 / JOSÉ ALFREDO CERDAS ESQUIVEL / BIG DATA

#SE REALIZAN LAS IMPORTACIONES NECESARIAS
import sys
import pyspark
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql import SparkSession

#REALIZAMOS LA CREACION DE LA CLASE Y DENTRO DE ELLA LOS SCHEMAS QUE NECESITAMOS

class Primer_Tarea:
    def __init__(self):

        
        #ESQUEMA DE CURSO
        self_schema_Curso = StructType([\
				StructField("Codigo", IntegerType()),\
				StructField("Creditos", IntegerType()),\
				StructField("Carrera", StringType())\
		])
        
        #ESQUEMA DE ESTUDIANTE
        self_schema_Estudiante = StructType([\
				StructField("Nombre", IntegerType()),\
				StructField("Carnet", IntegerType()),\
				StructField("Carrera", StringType())\
		])
        
        #ESQUEMA DE NOTA
        self_schema_Nota = StructType([\
				StructField("Carnet", IntegerType()),\
				StructField("CodigoCurso", IntegerType()),\
				StructField("Nota", StringType())\
		])
        
        self.sc = pyspark.SparkContext('local[*]')

        self.spark = Session \
			.builder \
            .appname("Tarea 1 Jose Alfredo")
            .getOrCreate()
            
    
    def obtenerDataFrames(self,archivo1,archivo2,archivo3):
        
        if (archivo1 == "Estudiante.csv"):
			dfest = self.spark.read.csv("Estudiante.csv", header=False, sep=",", schema=self.schema_Estudiante)
        
        if (archivo2 == "Curso.csv"):
			dfcurso = self.spark.read.csv("Curso.csv", header=False, sep=",", schema=self.schema_Curso)
            
        if (archivo3 == "Nota.csv"):
			dfnota = self.spark.read.csv("Nota.csv", header=False, sep=",", schema=self.schema_Nota)

        return dfest, dfcurso, dfnota
        
    def unirDataFrames(self,dfest,dfcurso,dfnota):
    
        joindf1 = dfcurso.join(dfnota,dfcurso.codigo==dfnota.codigo_curso).drop(dfcurso.carrera)
		joindf2 = joindf1.join(dfest,joindf1.carnet==dfest.carnet).drop(dfest.carnet)
        
        joindf2.show()
        
        return joindf2
        
    def agregarDatos(self,df):
        
        df = df.withColumn('Total',col('Nota')*col('Creditos'))
		df = df.groupBy('Carnet').agg( first(df.carrera).alias("Carrera"), (sum(df.total) / sum(df.creditos)).alias("Promedio") )
		df.show()
		
		return df
        
    def mejoresEstudiantes(self,joindf1):
	
		window = Window.partitionBy(joindf1['Carrera']).orderBy(joindf1['Promedio'].desc())
		resultado = joindf1.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 2)
		return resultado
    
    

