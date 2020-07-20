import sys
from primer_Tarea import Primer_Tarea

if __name__ == "__main__":

    
    if len(sys.argv) != 4:
        print("Necesita incluir al menos tres parametros ", file=sys.stderr)
        exit(-1)
    
    t1 = Primer_Tarea()
    
    dfest, dfcurso, dfnota = t1.obtenerDataFrames(sys.argv[1],sys.argv[2],sys.argv[3])
    joindf1 = t1.unirDataFrames(dfest,dfcurso,dfnota)
    df = t1.agregarDatos(joindf1)
    resdf = t1.mejoresEstudiantes(df)
    resdf.show(500,False);
    
    