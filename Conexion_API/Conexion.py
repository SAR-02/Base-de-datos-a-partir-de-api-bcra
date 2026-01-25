import pandas as pd
import numpy as np
from datetime import date
import requests
import warnings

from funciones import *
warnings.filterwarnings("ignore")
desde=str(date.today())


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

url_1="https://api.bcra.gob.ar/estadisticas/v4.0/monetarias"
json=requests.get(url_1,verify=False).json()
result=json.get("results")

info_var=[]
for i in result:
    if i.get("categoria") == "Principales Variables": 
        diccionario={"idVariable":i.get("idVariable"),"Descripcion":i.get("descripcion"),"primer_fecha":i.get("primerFechaInformada"),"ultima_fecha":i.get("ultFechaInformada")}
        info_var.append(diccionario)
    else:
        pass

lista_df=[]
for i in range(0,len(info_var)):
    lista_pag=[]
    offset=0
    limit=1000
    desde="2003-12-10"# FECHA
    salir=False
    while salir is False: 
        #busco info
        url_2=f'{url_1}/{info_var[i].get("idVariable")}?hasta={info_var[i].get("ultima_fecha")}&desde={desde}&offset={offset}&limit={limit}'
        try:
            conex=requests.get(url_2,verify=False)
            if conex.status_code==200:
                json=conex.json()
                info=json.get("results")
                if info is not None:
                    detalle=info[0].get("detalle")
                    #paginado
                    lista_pag.extend(detalle)
                    total=json.get("metadata").get("resultset").get("count")
                    offset+=limit

                    if offset>=total:
                        df=pd.DataFrame(lista_pag)
                        salir=True

                elif info is None and len(lista_pag)!=0:#por si una variable solo se procesa parcialmente, la 24 lo necesitaba para fechas previas al 2003-01-02
                    df = pd.DataFrame(lista_pag)
                    salir=True

                elif info is None and len(lista_pag)==0:
                    salir=True 
            else:#por si falla la conexion con alguna variable(TAMAR EN diciembre y principio de enero)
                salir=True
        except Exception as e:
                
                salir = True      
    #uno resultados mantener if porque se rompe fácil la api
    if len(lista_pag) > 0:
            df = pd.DataFrame(lista_pag)
            if 'fecha' in df.columns:
                df["fecha"]=pd.to_datetime(df["fecha"])
                df=df.set_index("fecha")
                df=df.rename(columns={"valor":f"{info_var[i].get("Descripcion")}"})
                lista_df.append(df)
            else:
               print() #nada

df_total=pd.concat(lista_df,join="outer",axis=1)
df_total=df_total.loc[:,~df_total.columns.duplicated()]
df_total=df_total.reset_index()

presidentes=[{"Presidente":"Javier Milei","inicio":"2023-12-10","fin":"2027-12-9"},
             {"Presidente":"Alberto Fernandez","inicio":"2019-12-10","fin":"2023-12-9"},
             {"Presidente":"Mauricio Macri","inicio":"2015-12-10","fin":"2019-12-9"},
             {"Presidente":"Cristina Kirchner","inicio":"2011-12-10","fin":"2015-12-9"},
             {"Presidente":"Cristina Kirchner","inicio":"2007-12-10","fin":"2011-12-9"},
             {"Presidente":"Nestor Kirchner","inicio":"2003-12-10","fin":"2007-12-9"},
             ]

eventos=[{"evento":"","fecha":"","descripción":"","comentario":""}]

df_presidencias=pd.DataFrame(presidentes)
df_info_var=pd.DataFrame(info_var)

crear_tabla_sql("localhost","variables_por_presidente",df_total,"variables_principales")
crear_tabla_sql("localhost","variables_por_presidente",df_presidencias,"presidencias")
crear_tabla_sql("localhost","variables_por_presidente",df_info_var,"info_variables")

