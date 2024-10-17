
# -*- coding: utf-8 -*-
"""Created on Fri Sep 20 14:08:46 2024

ALUMNOS: Emiliano Torres, Matilda Bartoli y Sofia Copaga
GRUPO : EQUIPO ROCKET
"""

import pandas as pd
from inline_sql import sql,sql_val
import matplotlib.pyplot as plt
import numpy as np

#%%===========================================================================
# IMPORTACIÓN DE LAS BASES DE DATOS
#=============================================================================

#DATOS SEDES
carpeta = "/Users/Usuario/Downloads/Tp_Labo/"
datos_basicos=pd.read_csv(carpeta+"lista-sedes.csv")
datos_completos=pd.read_csv(carpeta+"Datos_sedes_completos.csv")
#arreglamos la linea 16 manualmente (ya que generaba un error al importar dicha base)
datos_secciones=pd.read_csv(carpeta+"Datos_sedes_secciones.csv")

#DATOS MIGRACIONES
datos_migraciones=pd.read_csv(carpeta+"datos_migraciones.csv")


#%%===========================================================================
# FILTRADO DE DATOS
#=============================================================================


datos_basicos=sql^ """SELECT DISTINCT sede_id, pais_iso_3, UPPER(pais_castellano) as pais_castellano
                      FROM datos_basicos """

datos_completos=sql^ """SELECT DISTINCT sede_id, region_geografica , redes_sociales FROM datos_completos"""

datos_secciones=sql^ """SELECT DISTINCT sede_id,COUNT(*) as cantidad_secciones FROM datos_secciones GROUP BY sede_id"""

datos_basicos=sql^ """SELECT s.sede_id, s.pais_iso_3, 
                      s.pais_castellano, sec.cantidad_secciones
                      FROM datos_basicos AS s LEFT OUTER JOIN datos_secciones AS sec ON s.sede_id=sec.sede_id"""

#renombramos las columnas de datos_migraciones 
datos_migraciones.rename(columns={'Country Origin Code': 'origen',
                          'Migration by Gender Code': 'genero',
                          'Country Dest Code':'destino',
                          '2000 [2000]':'casos_2000',
                          '1960 [1960]':'casos_1960',
                          '1970 [1970]':'casos_1970',
                          '1980 [1980]':'casos_1980',
                          '1990 [1990]':'casos_1990'},inplace=True)

datos_migraciones=sql^ """SELECT DISTINCT origen, destino, casos_1960,casos_1970,casos_1980,casos_1990,casos_2000 
                          FROM datos_migraciones WHERE genero='TOT'"""
                          

#%%===========================================================================
# ANÁLISIS- CALIDAD DE DATOS
#=============================================================================

#DATOS MIGRACIONES. METODO GQM
#Goal: que los datos correspondientes a las columnas de casos por año posean datos de tipo numerico. 
#Question: ¿Cuál es la proporción de filas que tienen los datos correspondientes a las columnas de los casos por años igual al simbolo ".."? 
#Metric = 4% (aproximadamente)

null_migraciones=datos_migraciones[(datos_migraciones['casos_1960']=='..') | (datos_migraciones['casos_1970']=='..')
                                   |(datos_migraciones['casos_1980']=='..') | (datos_migraciones['casos_1990']=='..')
                                   |(datos_migraciones['casos_2000']=='..')]
metrica_migraciones=null_migraciones.shape[0]/datos_migraciones.shape[0]*100



#DATOS COMPLETOS . METODO GQM
#Goal: que el dato correspondiente a la red social de cada sede esté completo. 
#Question: ¿Cuál es la proporción de sedes que tienen el dato correspondiente a “redes_sociales” vacío?
#Metric : 24% (aproximadamente)

null_redes_sociales=(sql^ """Select redes_sociales FROM datos_completos WHERE redes_sociales IS NULL""").shape[0]
metrica_redes_sociales=null_redes_sociales/datos_completos.shape[0]*100

#DATOS BASICOS. METODO GQM
#Goal: que el dato correspondiente a la cantidad de secciones de cada sede esté completo.
#Question: ¿Cuál es la proporción de sedes que tienen el dato correspondiente a "cantidad_secciones" vacío?
#Metric : 37% (aproximadamente)

null_secciones=(sql^ """Select cantidad_secciones FROM datos_basicos WHERE cantidad_secciones  IS NULL""").shape[0]
metrica_secciones=null_secciones/datos_basicos.shape[0]*100


#%%===========================================================================
# MEJORA DE CALIDAD DE DATOS- TRATAMIENTO DE NULLS
#=============================================================================
#DATOS MIGRACIONES.
#remplazamos los nulls expresados como .. por 0
for index,rows in null_migraciones.iterrows():
    if datos_migraciones.loc[index,'casos_1960']=='..':
        datos_migraciones.loc[index,'casos_1960']=0
    
    if datos_migraciones.loc[index,'casos_1970']=='..':
        datos_migraciones.loc[index,'casos_1970']=0
    
    if datos_migraciones.loc[index,'casos_1980']=='..':
        datos_migraciones.loc[index,'casos_1980']=0
        
    if datos_migraciones.loc[index,'casos_1990']=='..':
        datos_migraciones.loc[index,'casos_1990']=0
    
    if datos_migraciones.loc[index,'casos_2000']=='..':
        datos_migraciones.loc[index,'casos_2000']=0 
        
#DATOS COMPLETOS
#rellenamos los valores None del campo redes_sociales por el separador
datos_completos=datos_completos.fillna(value="//")

#DATOS BASICOS
#completamos los nulls de las sedes sin secciones con 0
datos_basicos=datos_basicos.fillna(value="0")
        
#ANALISIS DE METRICAS POST TRATAMIENTO

#DATOS MIGRACIONES
#Metric: 0%
null_migraciones=datos_migraciones[(datos_migraciones['casos_1960']=='..') | (datos_migraciones['casos_1970']=='..')
                                   |(datos_migraciones['casos_1980']=='..') | (datos_migraciones['casos_1990']=='..')
                                   |(datos_migraciones['casos_2000']=='..')]
metrica_migraciones=null_migraciones.shape[0]/datos_migraciones.shape[0]

#DATOS COMPLETOS
#Metric : 0%
null_redes_sociales=(sql^ """Select redes_sociales FROM datos_completos WHERE redes_sociales IS NULL""").shape[0]
metrica_redes_sociales=null_redes_sociales/datos_completos.shape[0]

#DATOS BASICOS
#Metric: 0%
null_secciones=(sql^ """Select cantidad_secciones FROM datos_basicos WHERE cantidad_secciones  IS NULL""").shape[0]
metrica_secciones=null_secciones/datos_basicos.shape[0]



#%%===========================================================================
# MEJORA DE CALIDAD DE DATOS- ATRIBUTOS COMPUESTOS A ATÓMICOS
#=============================================================================
#DATOS COMPLETOS - COLUMNA redes_sociales

indexes=[] #futuras columnas del df
redes=[]  #futuras columnas del df
for index,row in datos_completos.iterrows():
   
    
   lista : str =datos_completos.loc[index]['redes_sociales']
   posicion_actual : int=0 #posicion de los caracteres
   longitud : int=len(lista) #longitud del la lista anidada a la relacion datos completos
   red : str="" #url de la red actual
   en_red : bool=True #esta leyendo un caracter perteneciente a una Url valida
   
   
   while lista[posicion_actual]==" ": #Url empezada en espacio
       posicion_actual+=1
       
   while  posicion_actual<longitud:
       if (not en_red and (longitud-posicion_actual>2)): #tratamiento del separador
           if (lista[posicion_actual]=="/" and lista[posicion_actual+1]=="/"):
               en_red : bool=True    
               posicion_actual+=4 
       #primero se pregunta si no es null, luego si esta en una url valida y luego si es una posicion valida del string    
       if ((lista[0]!="/") and en_red and posicion_actual<longitud):
           
           if ((lista[posicion_actual] == " ") and
               (lista[posicion_actual+1] == " ") and
               (lista[posicion_actual+2] == "/")): #fin url
               if not red in redes: #hay redes repetidas
                   indexes.append(datos_completos.iloc[index]['sede_id']) #se guarda una copia del index al que pertenece la red 
                   redes.append(red) #la url de la red
               red="" #inicializa una nueva red
               en_red=False #avisa que ya no esta mirando un caracter valido de url
           
           else:
               if lista[posicion_actual]==" ":
                   red+="%20" #las url no soportan espacios, asi que se codifican
               else:
                   red+=lista[posicion_actual]
       posicion_actual+=1
       
#armado del dataframe       
dict_sede_red_social : dict={'sede_id': indexes ,'url' : redes} 

red_social=pd.DataFrame(dict_sede_red_social)



#%%===========================================================================
# FILTRADO DE DATOS DEL DATAFRAME red_social
#=============================================================================
#%% FILTRO 1
#filtramos aquellas filas del dataFrame red_social donde el valor de la columna url no tiene una url "explicita" y
#tiene como valor un nombre de usuario donde la primera letra es el simbolo @ y las quitamos del dataFrame red_social. 

red_social_2 = sql^ """Select sede_id, url
            From red_social 
            Where url Like '@%'
            """
red_social = sql^ """
            SELECT rs.sede_id, rs.url
            FROM red_social AS rs
            LEFT JOIN red_social_2 AS r2 ON rs.sede_id = r2.sede_id AND rs.url = r2.url
            WHERE r2.sede_id IS NULL
            """
#convertimos los nombre de usuarios de instagram a url 
def convertir_a_url_instagram(url):
    if url in [
    "@embajada_argentina_en_bolivia",
     "@embargenqatar", 
     "@consuladoargentinoensalto", 
     "@argennicaragua",
     "@arg_clang",
     "@consuladoargentinoenroma", 
     "@argenflorianopolis", 
     "@argentinaindonesia", 
     "@embargenturquia"] :
        
        return f'https://instagram.com/{url[1:]}'
    else:
       return url

#aplicamos la función a la columna url
red_social_2['url'] = red_social_2['url'].apply(convertir_a_url_instagram)

##convertimos los nombres de usuario de facebook a url
def convertir_a_url_facebook(url):
    if url in [
            "@EmbajadaArgentinaBolivia", 
            "@Argentinaenturquia/", 
            "@ArgentinaEnChina", 
            "@ArgentinaEnHonduras", 
            "@ArgentinaEnNicaragua", 
            "@ArgEnRoma", 
            "@ArgentinaEnPanama"] :

        return f'https://facebook.com/{url[1:]}'
    else:
        return url

#aplicamos la función a la columna url
red_social_2['url'] = red_social_2['url'].apply(convertir_a_url_facebook)

#convertimos los nombre de usuarios de twitter a url
def convertir_a_url_twitter(url):
    if url in [
            "@ArEthiopia",
            "@ArgColombia",
            "@ARGenSenegal",
            "@ARGenTurquia",
            "@EmbaArgBolivia", 
            "@ArgenFao", 
            "@argenmiami", 
            "@ARGenHouston"] :

        return f'https://twitter.com/{url[1:]}'
    else:
        return url

#aplico la función a la columna url
red_social_2['url'] = red_social_2['url'].apply(convertir_a_url_twitter)

#borramos aquellas filas que tiene como url un dato no existente en la realidad
sede_a_eliminar_1 = "CASUN"
url_a_eliminar_1 = "@ArgentinaEnAsuncion"
sede_a_eliminar_2 = "EHOND"
url_a_eliminar_2= "@ArgHonduras"

red_social_2 = red_social_2[~((red_social_2['sede_id'] == sede_a_eliminar_1) & (red_social_2['url'] == url_a_eliminar_1))]
red_social_2 = red_social_2[~((red_social_2['sede_id'] == sede_a_eliminar_2) & (red_social_2['url'] == url_a_eliminar_2))]

#%% FILTRO 2
#filtramos aquellas filas del dataFrame red_social donde el valor de la columna url no tiene una url "explicita" y
#tiene como valor algo que no posea el simbolo @, o "https" o "/" y las quitamos del dataFrame red_social.
red_social_3 = sql^ """
    SELECT sede_id, url
    FROM red_social
    WHERE url NOT LIKE '%@%' 
      AND url NOT LIKE '%https%' 
      AND url NOT LIKE '%/%'
"""
red_social = sql^ """
            SELECT rs.sede_id, rs.url
            FROM red_social AS rs
            LEFT JOIN red_social_3 AS r3 ON rs.sede_id = r3.sede_id AND rs.url = r3.url
            WHERE r3.sede_id IS NULL
            """
#convertimos a url los datos que pueden ser convertidos bajo nuestro criterio y analisis
def convertir_a_url_instagram_2(url):
    if url in [
        "argentinaencolombia", 
        "argentinaenjamaica", 
        "embajadaargentinaenjapon", 
        "argenmozambique", 
        "arg_trinidad_tobago", 
        "consuladoargentinomia"
    ]:
        return f'https://instagram.com/{url}'
    else:
        return url

#aplicamos la función a la columna url
red_social_3['url'] = red_social_3['url'].apply(convertir_a_url_instagram_2)
#borramos aquellas filas donde la red social no refleja un dato de la realidad (para mejorar la calidad de nuestros datos)
sede_a_eliminar_3 = "CSCRS"
url_a_eliminar_3 = "cscrs2018"
sede_a_eliminar_4 = "CBARC"
url_a_eliminar_4= "Consulado%20%20Argentino%20%20en%20%20Barcelona"
sede_a_eliminar_5 = "EHOND"
url_a_eliminar_5 = "Embajada%20%20Argentina%20%20en%20%20Honduras"

red_social_3 = red_social_3[~((red_social_3['sede_id'] == sede_a_eliminar_3) & (red_social_3['url'] == url_a_eliminar_3))]
red_social_3 = red_social_3[~((red_social_3['sede_id'] == sede_a_eliminar_4) & (red_social_3['url'] == url_a_eliminar_4))]
red_social_3 = red_social_3[~((red_social_3['sede_id'] == sede_a_eliminar_5) & (red_social_3['url'] == url_a_eliminar_5))]

#%% FILTRO FINAL
#concatenamos los 3 dataFrames
red_social = pd.concat([red_social, red_social_2, red_social_3], ignore_index=True)


#%%===========================================================================
# CONSTRUCCION DE NUESTRA BASE DE DATOS SEGUN EL DER
#=============================================================================
#red_social lo armamos en la celda anterior
flujos_migratorios= datos_migraciones
sedes=sql^ """SELECT DISTINCT db.sede_id, db.pais_iso_3 ,db.pais_castellano AS pais, 
            dc.region_geografica,db.cantidad_secciones 
            FROM datos_basicos AS db INNER JOIN datos_completos AS dc ON db.sede_id=dc.sede_id"""



#%%===========================================================================
# PASAMOS EL MODELO RELACIONAL A 3FN
#=============================================================================
codigos_paises=sql^"""SELECT DISTINCT pais_iso_3, pais FROM sedes"""
ubicacion=sql^"""SELECT DISTINCT pais_iso_3, region_geografica FROM sedes"""
sedes=sql^"""SELECT DISTINCT sede_id, pais_iso_3,cantidad_secciones FROM sedes"""


#%%===========================================================================
# CONSULTAS
#=============================================================================
#%%
#Consulta i)
#contamos la cantidad de sedes argentinas en cada pais y la suma de sus secciones
sedes_por_paises=sql^"""SELECT DISTINCT cp.pais, COUNT(s.sede_id) AS sedes,
                            SUM(CAST(s.cantidad_secciones AS INTEGER)) as secciones
                            FROM sedes AS s
                            INNER JOIN codigos_paises AS cp ON s.pais_iso_3=cp.pais_iso_3
                            GROUP BY cp.pais
                            """ 
#calculamos el flujo de emigrantes de cada pais
flujo_emigrantes=sql^"""SELECT cp.pais, sum(CAST(fm.casos_2000 AS INTEGER)) AS emigrantes FROM flujos_migratorios AS fm
                        INNER JOIN codigos_paises cp ON cp.pais_iso_3= fm.origen
                        GROUP BY cp.pais 
                        """
#calculamos el flujo de imigrantes de cada pais                       
flujo_inmigrantes=sql^"""SELECT cp.pais, sum(CAST(fm.casos_2000 AS INTEGER)) AS inmigrantes FROM flujos_migratorios AS fm
                         INNER JOIN codigos_paises cp ON cp.pais_iso_3= fm.destino
                         GROUP BY cp.pais
                        """
#calculamos el flujo migratorio neto                        
flujo_migratorio_neto=sql^"""SELECT DISTINCT fe.pais,fe.emigrantes-fi.inmigrantes AS neto FROM flujo_emigrantes AS fe
                            INNER JOIN flujo_inmigrantes AS fi ON fe.pais=fi.pais """
#unimos todo en el resultado                            
dataframe_resultado_i=sql^ """SELECT DISTINCT spp.pais,spp.sedes,spp.secciones /spp.sedes AS 'Secciones promedio',
                            fmn.neto AS  'Flujo Migratorio Neto' FROM sedes_por_paises AS spp
                            INNER JOIN flujo_migratorio_neto AS fmn ON spp.pais=fmn.pais
                            ORDER BY spp.sedes DESC, spp.pais ASC"""

#%% Consulta ii)

paises_con_sedes_argentinas=sql^"""SELECT DISTINCT pais_iso_3 FROM sedes"""

#la cantidad de paises con sedes agrupadas por region
regiones=sql^ """SELECT DISTINCT u.region_geografica, count(p.pais_iso_3) AS paises FROM paises_con_sedes_argentinas AS p
                            INNER JOIN ubicacion AS u ON u.pais_iso_3=p.pais_iso_3
                            GROUP BY region_geografica
                            """
#calculamos la cantidad de emigrantes argentinos
flujo_emigrantes_por_pais=sql^"""SELECT p.pais_iso_3, SUM(CAST(fm.casos_2000 AS INTEGER)) AS flujo FROM paises_con_sedes_argentinas AS p
                                   INNER JOIN flujos_migratorios AS fm ON p.pais_iso_3=fm.destino WHERE fm.origen='ARG' AND p.pais_iso_3!='ARG'
                                   GROUP BY p.pais_iso_3
                                """
#agrupamos la cantidad de migrantes previamente calculada por region
flujo_emigrantes_por_regiones=sql^ """SELECT DISTINCT u.region_geografica, SUM(fepp.flujo) AS flujo_regional
                                      FROM flujo_emigrantes_por_pais AS fepp
                                      INNER JOIN ubicacion AS u ON fepp.pais_iso_3=u.pais_iso_3
                                      GROUP BY u.region_geografica"""
                                      
#unimos los datos previamente calculado, les ponemos el nombre y ordenamos como pedia la consigna
dataframe_resultado_ii=sql^ """SELECT DISTINCT fepg.region_geografica AS 'region geografica', 
                            r.paises AS 'Paises Con Sedes Argentinas',
                            fepg.flujo_regional/r.paises AS 'Promedio flujo con Argentina - Países con Sedes Argentinas'
                            FROM flujo_emigrantes_por_regiones AS fepg INNER JOIN regiones AS r 
                            ON r.region_geografica=fepg.region_geografica
                            ORDER BY "Promedio flujo con Argentina - Países con Sedes Argentinas" DESC""" 
                            
#%% Consulta iii)
redes_por_sedes=sql^"""SELECT DISTINCT sede_id, CASE WHEN url LIKE '%facebook%' THEN 'facebook' ELSE
                       CASE WHEN url LIKE '%Facebook%' THEN 'facebook' ELSE
                       CASE WHEN url LIKE '%instagram%' THEN 'instragram' ELSE 
                       CASE WHEN url LIKE '%Instagram%' THEN 'instagram' ELSE
                       CASE WHEN url LIKE '%twitter%' THEN 'twitter' ELSE
                       CASE WHEN url LIKE '%youtube%' THEN 'youtube' ELSE
                       CASE WHEN url LIKE '%linkedin%' THEN 'linkedin' ELSE
                       CASE WHEN url LIKE '%gmail%' THEN 'gmail' ELSE
                       CASE WHEN url LIKE '%flickr%' THEN 'flickr' END END END END END END END END END AS red FROM red_social """ 

#unimos las sedes con sus respectivos paises
sedes_paises=sql^"""SELECT DISTINCT cp.pais, rps.sede_id FROM redes_por_sedes AS rps
                INNER JOIN sedes AS s ON rps.sede_id=s.sede_id  
                INNER JOIN codigos_paises AS cp ON s.pais_iso_3=cp.pais_iso_3"""

#unimos los datos previamente calculado, les ponemos el nombre y ordenamos como pedia la consigna
dataframe_resultado_iii=sql^"""SELECT DISTINCT sp.pais, COUNT(DISTINCT rps.red) AS 'redes distintas' 
                           FROM redes_por_sedes AS rps
                           INNER JOIN sedes_paises AS sp ON rps.sede_id=sp.sede_id 
                           WHERE rps.red IS NOT NULL
                           GROUP BY sp.pais """

#%% Consulta iv)
redes_por_sedes_2=sql^"""SELECT DISTINCT sede_id, CASE WHEN url LIKE '%facebook%' THEN 'facebook' ELSE
                       CASE WHEN url LIKE '%Facebook%' THEN 'facebook' ELSE
                       CASE WHEN url LIKE '%instagram%' THEN 'instragram' ELSE 
                       CASE WHEN url LIKE '%Instagram%' THEN 'instagram' ELSE
                       CASE WHEN url LIKE '%twitter%' THEN 'twitter' ELSE
                       CASE WHEN url LIKE '%youtube%' THEN 'youtube' ELSE
                       CASE WHEN url LIKE '%linkedin%' THEN 'linkedin' ELSE
                       CASE WHEN url LIKE '%gmail%' THEN 'gmail' ELSE
                       CASE WHEN url LIKE '%flickr%' THEN 'flickr' END END END END END END END END END AS red, url FROM red_social """ 
                     
#reutilizamos la tabla sedes_paises del ejercicio anterior           

dataframe_resultado_iv=sql^"""SELECT DISTINCT sp.pais, rps.sede_id, rps.red, rps.url 
                           FROM redes_por_sedes_2 AS rps 
                           INNER JOIN sedes_paises AS sp ON rps.sede_id=sp.sede_id
                           WHERE rps.red IS NOT NULL 
                           ORDER BY sp.pais ASC, rps.sede_id ASC, rps.red ASC, rps.url ASC """
                           


#%%===========================================================================
# GRAFICOS
#=============================================================================
#%%
# Grafico i)Cantidad de sedes por región geográfica

#buscamos la cantidad de sedes por regiones
sedes_por_codigo_pais=sql^ """SELECT DISTINCT pais_iso_3, COUNT(sede_id) AS cantidad_de_sedes
                                FROM sedes GROUP BY pais_iso_3 """

#agrupamos por regiones, no lo hacemos en una consulta ya que al no unir por claves se generan tuplas espureas
#lo que modificaria el valor del count
sedes_por_region_aux=sql^ """SELECT DISTINCT u.region_geografica, s.cantidad_de_sedes 
                            FROM sedes_por_codigo_pais AS s 
                            INNER JOIN ubicacion AS u
                            ON s.pais_iso_3 = u.pais_iso_3
                             """
                                            
sedes_por_region=sql^ """SELECT DISTINCT s.region_geografica, SUM(s.cantidad_de_sedes) AS sedes
                            FROM sedes_por_region_aux AS s
                            GROUP BY s.region_geografica ORDER BY sedes DESC """
fig, ax = plt.subplots(figsize=(10,8))
ax.bar(x=sedes_por_region['region_geografica'],height=sedes_por_region['sedes'],label='sedes')
ax.legend() #genera la legenda usando la label
ax.set_xlabel('Region geografica',fontsize=12)
ax.set_ylabel('Cantidad de sedes',fontsize=12)
ax.set_xticklabels(sedes_por_region['region_geografica'],rotation=60) 
ax.set_title('Cantidad de sedes por región geográfica',fontsize=19)
ax.legend()
fig.tight_layout() #ajusta el tamaño del figure para que las etiquetas entren bien

#%%Grafico ii)Cantidad de flujo migratorio promedio por region
#Hacemos una tabla con origenes y destinos con sedes argentinas
paises_objetivo=sql^ """SELECT DISTINCT origen,destino FROM flujos_migratorios
                        INNER JOIN paises_con_sedes_argentinas ON origen=pais_iso_3 
                        WHERE destino IN (SELECT DISTINCT pais_iso_3 FROM paises_con_sedes_argentinas)"""
#Necesitamos sumar todas las emigraciones independientemente del año en cada pais con almenos una sede argentina
flujo_emigratorio=sql^ """SELECT DISTINCT p.origen , 
                          SUM(CAST(fm.casos_1960 AS INTEGER)+
                           CAST(fm.casos_1970 AS INTEGER)+
                           CAST(fm.casos_1980 AS INTEGER)+
                           CAST(fm.casos_1990 AS INTEGER)+
                           CAST(fm.casos_2000 AS INTEGER)) AS flujo_emi
                          FROM paises_objetivo AS p 
                          INNER JOIN flujos_migratorios AS fm
                          ON p.origen= fm.origen AND p.destino=fm.destino 
                          GROUP BY p.origen 
                         """
#realizamos lo mismo con las inmigraciones
flujo_inmigratorio=sql^ """SELECT DISTINCT p.destino , 
                          SUM(CAST(fm.casos_1960 AS INTEGER)+
                           CAST(fm.casos_1970 AS INTEGER)+
                           CAST(fm.casos_1980 AS INTEGER)+
                           CAST(fm.casos_1990 AS INTEGER)+
                           CAST(fm.casos_2000 AS INTEGER)) AS flujo_inmi
                          FROM paises_objetivo AS p 
                          INNER JOIN flujos_migratorios AS fm
                          ON p.origen= fm.origen AND p.destino=fm.destino 
                          GROUP BY p.destino 
                        """
#Calculamos el promedio
flujo_migratorio_paises=sql^""" SELECT fe.origen, (flujo_emi-flujo_inmi)/5 AS flujo_migratorio 
                                FROM flujo_emigratorio AS fe
                                INNER JOIN flujo_inmigratorio AS fi ON fe.origen=fi.destino"""

#lo agrupamos por regiones
flujo_promedio_por_regiones=sql^"""SELECT DISTINCT u.region_geografica, flujo_migratorio
                                FROM ubicacion AS u INNER JOIN flujo_migratorio_paises AS f
                                ON u.pais_iso_3=f.origen
                                """

#Van  a ser la regiones sobre las que vamos a hacer los boxplot
regiones_objetivo=sql^"""SELECT DISTINCT region_geografica FROM flujo_promedio_por_regiones """


cantidad_regiones=regiones_objetivo.shape[0]


valores_por_regiones=[]
#obtenemos todos los datos_separados por regiones
for index, row in regiones_objetivo.iterrows():
    region=regiones_objetivo.loc[index,'region_geografica']
    df_boxplot=sql^ """SELECT flujo_migratorio FROM flujo_promedio_por_regiones WHERE region_geografica=$region"""        
    #pasamos a lista por que matplotlib no maneja bien los df para hacer los labels
    lista_boxplot=[]
    for index_df,row_df in df_boxplot.iterrows():
        lista_boxplot.append(df_boxplot.loc[index_df,'flujo_migratorio'])
    valores_por_regiones.append((region,lista_boxplot))
    #decidimos estructurar los datos en tuplas ya que necesitamos que al ordenar ambos se ordenen segun la mediana 

#los ordenamos segun la mediana (de forma decreciente)
valores_regiones_ordenados=[]
valores_ordendados=[]
inf=10e12
maximo=-inf
for i in range(len(valores_por_regiones)):
    for j in range(len(valores_por_regiones)):
        candidato=np.median(valores_por_regiones[j][1])
        if  j not in valores_ordendados and candidato>=maximo : # buscamos el valor maximo de mediana
            maximo=candidato
            maximo_index=j
    valores_regiones_ordenados.append(valores_por_regiones[maximo_index]) # los valores ordenados
    valores_ordendados.append(maximo_index) #guardamos las medianas que ya fueron ordenadas 
    maximo=-inf

#separamos en una lista de regiones y otra de valores para los datos ya ordenados
region_ordenada=[] 
datos_region_ordenados=[]
for i in range (len(valores_regiones_ordenados)):
    region_ordenada.append(valores_regiones_ordenados[i][0])
    datos_region_ordenados.append(valores_regiones_ordenados[i][1])


#los_graficamos en orden
fig, ax =plt.subplots(figsize=(18,6))
ax.boxplot(datos_region_ordenados,label='mediana')
ax.scatter(x=[1,2,3,4,5,6,7,8,9], y=[np.mean(x) for x in datos_region_ordenados], label='media', color='green')
ax.set_xticks([1,2,3,4,5,6,7,8,9])
ax.set_xticklabels(region_ordenada) 
plt.xticks(rotation=30)
plt.tight_layout()
ax.legend()

#%% Grafico iii)Flujos migratorios hacia argentina en el año 2000 y cantidad de sedes













