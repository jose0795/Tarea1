import sys
import pytest
import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from tarea1 import Tarea1

t1 = Primer_Tarea()

def test_obtenerDF():
    
    file1 = 'est.csv'
    file2 = 'curso.csv'
    file3 = 'nota.csv'
    
    df1, df2, df3 = t1.obtenerDataFrames(file1,file2,file3)
	assert isinstance(df1,pyspark.sql.DataFrame)
    assert isinstance(df2,pyspark.sql.DataFrame)
    assert isinstance(df3,pyspark.sql.DataFrame)
    
def test_unirDatosDF():
    
    total_schema = StructType([\
				StructField("Codigo", IntegerType()),\
				StructField("Creditos", IntegerType()),\
				StructField("Carnet", IntegerType()),\
				StructField("CodigoCurso", IntegerType()),\
				StructField("Nota", IntegerType()),\
				StructField("Nombre", StringType()),\
				StructField("Carrera", StringType()),\
	])
    
    lista_est = [['Abelardo',12345, 'Civil'], ['Enrique',67890, 'Sistemas'], ['Francesca',11111, 'Industrial'],['Randall',99999, 'Mercadeo']]
	lista_nota = [[12345,1000, 60], [67890,2000, 75], [11111,3000, 90],[99999,4000, 100]]
	lista_curso = [[1000,1, 'Civil'], [2000,2, 'Sistemas'], [3000,3, 'Industrial'],[4000,4, 'Mercadeo']]
	
	df_est = t1.spark.createDataFrame(lista_est,schema=t1.schema_Estudiante)
	df_nota = t1.spark.createDataFrame(lista_nota,schema=t1.schema_Nota)
	df_curso = t1.spark.createDataFrame(lista_curso,schema=t1.schema_Curso)

	joindf1 = t1.unirDataFrames(df_est,df_curso,df_nota)
	print(joindf1.schema)
	assert joindf1.schema == total_schema

def test_estudianteSinCurso():
    
    total_schema = StructType([\
				StructField("Codigo", IntegerType()),\
				StructField("Creditos", IntegerType()),\
				StructField("Carnet", IntegerType()),\
				StructField("CodigoCurso", IntegerType()),\
				StructField("Nota", IntegerType()),\
				StructField("Nombre", StringType()),\
				StructField("Carrera", StringType()),\
	])
    
    lista_est = [['Abelardo',12345, 'Civil'], ['Enrique',67890, 'Sistemas'], ['Francesca',11111, 'Industrial'],['Randall',99999, 'Mercadeo'],['Richard',77777,'Fisica']]
	lista_nota = [[12345,1000, 60], [67890,2000, 75], [11111,3000, 90],[99999,4000, 100]]
	lista_curso = [[1000,1, 'Civil'], [2000,2, 'Sistemas'], [3000,3, 'Industrial'],[4000,4, 'Mercadeo']]
	
	df_est = t1.spark.createDataFrame(lista_est,schema=t1.schema_Estudiante)
	df_nota = t1.spark.createDataFrame(lista_nota,schema=t1.schema_Nota)
	df_curso = t1.spark.createDataFrame(lista_curso,schema=t1.schema_Curso)
    
    joindf1 = t1.unirDataFrames(df_est,df_curso,df_nota)
	assert joindf1.schema == total_schema
    
def test_cursoSinEstudiante():
    
    total_schema = StructType([\
				StructField("Codigo", IntegerType()),\
				StructField("Creditos", IntegerType()),\
				StructField("Carnet", IntegerType()),\
				StructField("CodigoCurso", IntegerType()),\
				StructField("Nota", IntegerType()),\
				StructField("Nombre", StringType()),\
				StructField("Carrera", StringType()),\
	])
    
    lista_est = [['Abelardo',12345, 'Civil'], ['Enrique',67890, 'Sistemas'], ['Francesca',11111, 'Industrial'],['Randall',99999, 'Mercadeo']]
	lista_nota = [[12345,1000, 60], [67890,2000, 75], [11111,3000, 90],[99999,4000, 100]]
	lista_curso = [[1000,1, 'Civil'], [2000,2, 'Sistemas'], [3000,3, 'Industrial'],[4000,4, 'Mercadeo'],[5000,5,'Fisica']
	
	df_est = t1.spark.createDataFrame(lista_est,schema=t1.schema_Estudiante)
	df_nota = t1.spark.createDataFrame(lista_nota,schema=t1.schema_Nota)
	df_curso = t1.spark.createDataFrame(lista_curso,schema=t1.schema_Curso)
    
    joindf1 = t1.unirDataFrames(df_est,df_curso,df_nota)
	assert joindf1.schema == total_schema
    
def test_notaSinCurso():
    
    total_schema = StructType([\
				StructField("Codigo", IntegerType()),\
				StructField("Creditos", IntegerType()),\
				StructField("Carnet", IntegerType()),\
				StructField("CodigoCurso", IntegerType()),\
				StructField("Nota", IntegerType()),\
				StructField("Nombre", StringType()),\
				StructField("Carrera", StringType()),\
	])
    
    lista_est = [['Abelardo',12345, 'Civil'], ['Enrique',67890, 'Sistemas'], ['Francesca',11111, 'Industrial'],['Randall',99999, 'Mercadeo']]
	lista_nota = [[12345,1000, 60], [67890,2000, 75], [11111,3000, 90],[99999,4000, 100],[99999,5000,80]]
	lista_curso = [[1000,1, 'Civil'], [2000,2, 'Sistemas'], [3000,3, 'Industrial'],[4000,4, 'Mercadeo']]
    
    df_est = t1.spark.createDataFrame(lista_est,schema=t1.schema_Estudiante)
	df_nota = t1.spark.createDataFrame(lista_nota,schema=t1.schema_Nota)
	df_curso = t1.spark.createDataFrame(lista_curso,schema=t1.schema_Curso)
    
    joindf1 = t1.unirDataFrames(df_est,df_curso,df_nota)
	assert joindf1.schema == total_schema
    
def test_estudianteRepiteCurso():
    
    total_schema = StructType([\
				StructField("Codigo", IntegerType()),\
				StructField("Creditos", IntegerType()),\
				StructField("Carnet", IntegerType()),\
				StructField("CodigoCurso", IntegerType()),\
				StructField("Nota", IntegerType()),\
				StructField("Nombre", StringType()),\
				StructField("Carrera", StringType()),\
	])
    
    lista_est = [['Abelardo',12345, 'Civil'], ['Enrique',67890, 'Sistemas'], ['Francesca',11111, 'Industrial'],['Randall',99999, 'Mercadeo']]
	lista_nota = [[12345,1000, 60], [67890,2000, 75], [11111,3000, 90],[99999,4000, 65],[99999,4000,85]]
	lista_curso = [[1000,1, 'Civil'], [2000,2, 'Sistemas'], [3000,3, 'Industrial'],[4000,4, 'Mercadeo']]
    
    df_est = t1.spark.createDataFrame(lista_est,schema=t1.schema_Estudiante)
	df_nota = t1.spark.createDataFrame(lista_nota,schema=t1.schema_Nota)
	df_curso = t1.spark.createDataFrame(lista_curso,schema=t1.schema_Curso)
    
    joindf1 = t1.unirDataFrames(df_est,df_curso,df_nota)
	assert joindf1.schema == total_schema   
    
def test_cursoValorNulo():

    total_schema = StructType([\
				StructField("Codigo", IntegerType()),\
				StructField("Creditos", IntegerType()),\
				StructField("Carnet", IntegerType()),\
				StructField("CodigoCurso", IntegerType()),\
				StructField("Nota", IntegerType()),\
				StructField("Nombre", StringType()),\
				StructField("Carrera", StringType()),\
	])
    
    lista_est = [['Abelardo',12345, 'Civil'], ['Enrique',67890, 'Sistemas'], ['Francesca',11111, 'Industrial'],['Randall',99999, 'Mercadeo']]
	lista_nota = [[12345,1000, 60], [67890,2000, 75], [11111,3000, 90],[99999,4000, 100]]
	lista_curso = [[1000,1, 'Civil'], [2000,2, 'Sistemas'], [3000,3, 'Industrial'],[None,4, 'Mercadeo']]
    
    df_est = t1.spark.createDataFrame(lista_est,schema=t1.schema_Estudiante)
	df_nota = t1.spark.createDataFrame(lista_nota,schema=t1.schema_Nota)
	df_curso = t1.spark.createDataFrame(lista_curso,schema=t1.schema_Curso)
    
    joindf1 = t1.unirDataFrames(df_est,df_curso,df_nota)
	assert joindf1.schema == total_schema
    
def test_estudianteValorNulo():

    total_schema = StructType([\
				StructField("Codigo", IntegerType()),\
				StructField("Creditos", IntegerType()),\
				StructField("Carnet", IntegerType()),\
				StructField("CodigoCurso", IntegerType()),\
				StructField("Nota", IntegerType()),\
				StructField("Nombre", StringType()),\
				StructField("Carrera", StringType()),\
	])
    
    lista_est = [['Abelardo',12345, 'Civil'], [None,67890, 'Sistemas'], ['Francesca',11111, 'Industrial'],['Randall',99999, 'Mercadeo']]
	lista_nota = [[12345,1000, 60], [67890,2000, 75], [11111,3000, 90],[99999,4000, 100]]
	lista_curso = [[1000,1, 'Civil'], [2000,2, 'Sistemas'], [3000,3, 'Industrial'],[4000,4, 'Mercadeo']]
    
    df_est = t1.spark.createDataFrame(lista_est,schema=t1.schema_Estudiante)
	df_nota = t1.spark.createDataFrame(lista_nota,schema=t1.schema_Nota)
	df_curso = t1.spark.createDataFrame(lista_curso,schema=t1.schema_Curso)
    
    joindf1 = t1.unirDataFrames(df_est,df_curso,df_nota)
	assert joindf1.schema == total_schema
    
    
def test_agregaDatos():

    total_schema = StructType([\
				StructField("Carnet", IntegerType()),\
				StructField("Carrera", StringType()),\
				StructField("Promedio", DoubleType())\
	])
    
    agregar_schema = StructType([\
				StructField("Codigo", IntegerType()),\
				StructField("Creditos", IntegerType()),\
				StructField("Carnet", IntegerType()),\
				StructField("CodigoCurso", IntegerType()),\
				StructField("Nota", IntegerType()),\
				StructField("Nombre", StringType()),\
				StructField("Carrera", StringType()),\
	])
    
    lista_agregar = [[1000,1,12346,1000,65,'Bernardo','Industrial'], [2000,2,12346,2000,80,'Bernardo','Industrial'],[2000,2,12348,2000,85,'Francesca','Industrial'],[4000,3,12349,4000,60,'Randall','Fisica'],[5000,4,12350,5000,95,'Randall','Fisica']]
	df_agregar = t1.spark.createDataFrame(lista_agregar,schema=agregar_schema)
	
	total_df = t1.agregarDatos(df_agregar)
	assert  total_df.schema == total_schema

def test_mejoresEstudiantes():
    
    total_schema = StructType([\
				StructField("Carnet", IntegerType()),\
				StructField("Carrera", StringType()),\
				StructField("Promedio", DoubleType())\
                StructField("rank", IntegerType())\
	])
    
    datos_schema = StructType([\
				StructField("Carnet", IntegerType()),\
				StructField("Carrera", StringType()),\
                StructField("Promedio", DoubleType()),\
	])
    
    lista_agregar = [[19876,'Fisica',66.34],[19877,'Civil',85.91],[19877,'Civil',90.12],[19878,'Civil',77.89]]
	df_agregar = t1.spark.createDataFrame(lista_agregar,schema=datos_schema)
    
    total_df = t1.mejoresEstudiantes(df_agregar)
	assert  total_df.schema == total_schema
    
def test_VerificarDosMejoresPromedios():
    
    datos_schema = StructType([\
				StructField("Carnet", IntegerType()),\
				StructField("Carrera", StringType()),\
                StructField("Promedio", DoubleType()),\
	])
    
    lista_agregar = [[19876,'Civil',77.89],[19877,'Civil',85.91],[19878,'Civil',90.12],[19879,'Fisica',66.34],[19880,'Civil',89.33]]
	df_agregar = t1.spark.createDataFrame(lista_agregar,schema=datos_schema)
    
    total_df = t1.mejoresEstudiantes(df_agregar)
    maximo = total_df.agg({"rank": "max"}).collect()[0][0]
    assert  maximo <= 2	