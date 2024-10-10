#%% Import y datos crudos
#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""Created on Fri Sep 20 14:08:46 2024

@author: Emiliano Torres, Matilda Bartoli y Sofia Copaga
"""

import pandas as pd
from inline_sql import sql,sql_val
#datos sedes
datos_basicos=pd.read_csv(".\\lista-sedes.csv")
datos_completos=pd.read_csv(".\\Datos_sedes_completos.csv")
#arreglamos la linea 16 manualmente
datos_secciones=pd.read_csv(".\\Datos_sedes_secciones.csv")

#datos migraciones
datos_migraciones=pd.read_csv(".\\datos_migraciones.csv")

#%% Filtrado

datos_basicos=sql^ """SELECT DISTINCT sede_id, pais_iso_3, pais_castellano,sede_tipo 
                      FROM datos_basicos """

datos_completos=sql^ """SELECT DISTINCT sede_id, region_geografica , redes_sociales FROM datos_completos"""

datos_secciones=sql^ """SELECT DISTINCT sede_id,COUNT(tipo_seccion) as seccion FROM datos_secciones GROUP BY sede_id"""

datos_basicos=sql^ """SELECT s.sede_id, s.pais_iso_3, 
                      s.pais_castellano,s.sede_tipo, sec.seccion 
                      FROM datos_basicos AS s LEFT OUTER JOIN datos_secciones AS sec ON s.sede_id=sec.sede_id"""

#renombramos porque tienen espacios los nombres

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
#%% Calidad de datos
#Goal: Tener valores numericos en las colummas casos para poder trabajar con esos datos
#Question: ¿Cuántos '..' en relacion a valores numericos tenemos en la tabla?
#cantidad de nulls
null_migraciones=datos_migraciones[(datos_migraciones['casos_1960']=='..') | (datos_migraciones['casos_1970']=='..')
                                   |(datos_migraciones['casos_1980']=='..') | (datos_migraciones['casos_1990']=='..')
                                   |(datos_migraciones['casos_2000']=='..')]
metrica_migraciones=null_migraciones.shape[0]/datos_migraciones.shape[0]
#la relacion es muy chica lo que es muy buen signo


#Goal Tener todos los datos de las url sin valores nulls, ya que son importantes para nuestro trabajo
#Question: ¿Cuántos nulls en relacion a todas las url en la tabla?
null_redes_sociales=(sql^ """Select redes_sociales FROM datos_completos WHERE redes_sociales IS NULL""").shape[0]
metrica_redes_sociales=null_redes_sociales/datos_completos.shape[0]
#tenemos casi un 25 porciento de valores null en redes sociales
#Goal Tener todos los datos de las cantidad de secciones
#Question: ¿Cuántos datos nulls tenemos en seccion en relacion los datos totaltes tenemos en la tabla?
null_secciones=(sql^ """Select seccion FROM datos_basicos WHERE seccion IS NULL""").shape[0]
metrica_secciones=null_secciones/datos_basicos.shape[0]
#vemos una proporcion de 0,37 aproximadamente

#%% tratamiento de nulls
#Remplazamos los nulls expresados como .. por 0
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
        
#rellenamos los valores None del campo redes_sociales por el separador
datos_completos=datos_completos.fillna(value="//")

#Completamos los nulls de las sedes sin secciones con 0
datos_basicos=datos_basicos.fillna(value="0")
        
#volvemos a revisar las metricas
null_migraciones=datos_migraciones[(datos_migraciones['casos_1960']=='..') | (datos_migraciones['casos_1970']=='..')
                                   |(datos_migraciones['casos_1980']=='..') | (datos_migraciones['casos_1990']=='..')
                                   |(datos_migraciones['casos_2000']=='..')]
metrica_migraciones=null_migraciones.shape[0]/datos_migraciones.shape[0]

null_redes_sociales=(sql^ """Select redes_sociales FROM datos_completos WHERE redes_sociales IS NULL""").shape[0]
metrica_redes_sociales=null_redes_sociales/datos_completos.shape[0]
#tenemos un 0 porciento de valores null en redes sociales

null_secciones=(sql^ """Select seccion FROM datos_basicos WHERE seccion IS NULL""").shape[0]
metrica_secciones=null_secciones/datos_basicos.shape[0]
# tenemos 0 nulls luego del tratamiento
#%% Separando la lista anidada

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
dict_sede_red_social : dict={'sede_id': indexes ,'redes' : redes} 

red_social=pd.DataFrame(dict_sede_red_social)
    
#%% Contruimos base de datos segun el der

#red_social lo armamos en la celda anterior
paises= datos_migraciones
sedes=sql^ """SELECT DISTINCT db.sede_id, db.pais_iso_3 ,db.pais_castellano AS pais, 
            dc.region_geografica, db.sede_tipo,db.seccion 
            FROM datos_basicos AS db INNER JOIN datos_completos AS dc ON db.sede_id=dc.sede_id"""

#%% Lo llevamos todo a 3FN
codigos_paises=sql^"""SELECT DISTINCT pais_iso_3, pais FROM sedes"""
ubicacion=sql^"""SELECT DISTINCT pais_iso_3, region_geografica FROM sedes"""
sedes=sql^"""SELECT DISTINCT sede_id, pais_iso_3,sede_tipo,seccion FROM sedes"""


#%% Ejercicios Consultas SQL i
#contamos la cantidad de sedes argentinas en cada pais y la suma de sus secciones
sedes_por_paises=sql^"""SELECT DISTINCT cp.pais, COUNT(s.sede_id) AS sedes,
                            SUM(CAST(s.seccion AS INTEGER)) as secciones
                            FROM sedes AS s
                            INNER JOIN codigos_paises AS cp ON s.pais_iso_3=cp.pais_iso_3
                            GROUP BY cp.pais
                            """ 




#calculamos el flujo de emigrantes de cada pais
flujo_emigrantes=sql^"""SELECT cp.pais, sum(CAST(casos_2000 AS INTEGER)) AS emigrantes FROM paises AS p
                        INNER JOIN codigos_paises cp ON cp.pais_iso_3= p.origen
                        GROUP BY cp.pais 
                        """
#calculamos el flujo de emigrantes de cada pais                       
flujo_inmigrantes=sql^"""SELECT cp.pais, sum(CAST(casos_2000 AS INTEGER)) AS inmigrantes FROM paises AS p
                         INNER JOIN codigos_paises cp ON cp.pais_iso_3= p.destino
                         GROUP BY cp.pais
                        """
#calculamos el flujo migratorio neto                        
flujo_migratorio_neto=sql^"""SELECT DISTINCT fe.pais,fe.emigrantes-fi.inmigrantes AS neto FROM flujo_emigrantes AS fe
                            INNER JOIN flujo_inmigrantes AS fi ON fe.pais=fi.pais """
#Unimos todo en el resultado                            
dataframe_resultado=sql^ """SELECT DISTINCT spp.pais,spp.sedes,spp.secciones/spp.sedes AS 'Secciones promedio',
                            fmn.neto AS  'Flujo Migratorio Neto' FROM sedes_por_paises AS spp
                            INNER JOIN flujo_migratorio_neto AS fmn ON spp.pais=fmn.pais
                            ORDER BY spp.sedes DESC, spp.pais ASC"""

#%% Ejercicio Consultas SQL ii

paises_con_sedes_argentinas=sql^"""SELECT DISTINCT pais_iso_3 FROM sedes"""
#la cantidad de paises con sedes agrupadas por region
regiones=sql^ """SELECT DISTINCT u.region_geografica, count(p.pais_iso_3) AS paises FROM paises_con_sedes_argentinas AS p
                            INNER JOIN ubicacion AS u ON u.pais_iso_3=p.pais_iso_3
                            GROUP BY region_geografica
                            """

flujo_emigrantes_por_pais=sql^"""SELECT p.pais_iso_3, SUM(CAST(paises.casos_2000 AS INTEGER)) AS flujo FROM paises_con_sedes_argentinas AS p
                                   INNER JOIN paises ON p.pais_iso_3=paises.destino WHERE paises.origen='ARG' AND p.pais_iso_3!='ARG'
                                   GROUP BY p.pais_iso_3
                                """
                                
flujo_emigrantes_por_regiones=sql^ """SELECT DISTINCT u.region_geografica, SUM(fepp.flujo) AS flujo_regional
                                      FROM flujo_emigrantes_por_pais AS fepp
                                      INNER JOIN ubicacion AS u ON fepp.pais_iso_3=u.pais_iso_3
                                      GROUP BY u.region_geografica"""
                                      
dataframe_resultado=sql^ """SELECT DISTINCT fepg.region_geografica AS 'region geografica', 
                            r.paises AS 'Paises Con Sedes Argentinas',
                            fepg.flujo_regional/r.paises AS 'Promedio flujo con Argentina - Países con Sedes Argentinas'
                            FROM flujo_emigrantes_por_regiones AS fepg INNER JOIN regiones AS r 
                            ON r.region_geografica=fepg.region_geografica
                            ORDER BY 'Promedio flujo con Argentina - Países con Sedes Argentinas' DESC""" 
                            
#%% Ejercicio Consultas SQL iii
# no son todas pero hay un buen cubrimiento
redes_por_sedes=sql^"""SELECT DISTINCT sede_id, CASE WHEN redes LIKE '%facebook%' THEN 'facebook' ELSE
                       CASE WHEN redes LIKE '%instagram%' THEN 'instragram' ELSE 
                       CASE WHEN redes LIKE '%twitter%' THEN 'twitter' ELSE
                       CASE WHEN redes LIKE '%youtube%' THEN 'youtube'  END END END END AS red FROM red_social """ 

#unimos las sedes con sus respectivos paises
sedes_paises=sql^"""SELECT DISTINCT cp.pais, rps.sede_id FROM redes_por_sedes AS rps
               INNER JOIN sedes AS s ON rps.sede_id=s.sede_id  
               INNER JOIN codigos_paises AS cp ON s.pais_iso_3=cp.pais_iso_3"""
#Hay nulls porque no identificamos todas las redes sociales
dataframe_resultado=sql^"""SELECT DISTINCT sp.pais, COUNT(DISTINCT rps.red) AS 'redes distintas' 
                           FROM redes_por_sedes AS rps
                           INNER JOIN sedes_paises AS sp ON rps.sede_id=sp.sede_id 
                           WHERE rps.red IS NOT NULL
                           GROUP BY sp.pais """

#%% Ejercicio Consultass SQL iv
#similar al punto anterior pero añadimos las url
redes_por_sedes=sql^"""SELECT DISTINCT sede_id, CASE WHEN redes LIKE '%facebook%' THEN 'facebook' ELSE
                       CASE WHEN redes LIKE '%instagram%' THEN 'instragram' ELSE 
                       CASE WHEN redes LIKE '%twitter%' THEN 'twitter' ELSE
                       CASE WHEN redes LIKE '%youtube%' THEN 'youtube'  END END END END AS red, 
                       redes AS url
                       FROM red_social """ 
#reutilizamos la tabla sedes_paises del ejercicio anterior           
dataframe_resultado=sql^"""SELECT DISTINCT sp.pais, rps.sede_id, rps.red, rps.url 
                           FROM redes_por_sedes AS rps 
                           INNER JOIN sedes_paises AS sp ON rps.sede_id=sp.sede_id
                           WHERE rps.red IS NOT NULL 
                           ORDER BY sp.pais ASC, rps.sede_id ASC, rps.red ASC, rps.url ASC """
















