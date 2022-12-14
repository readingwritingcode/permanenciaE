#!/usr/bin/python3

# utils
import unidecode
from fuzzywuzzy import fuzz

import pandas as pd
from pandas.errors import InvalidIndexError

import os
import math
from pathlib import Path

import pickle

# variables 
#format = {"EMPLEOS":"{:,.0f}", "EMPLEOS_x":"{:,.0f}", "EMPLEOS_y":"{:,.0f}", "VENTAS": "${:,.2f}", "VENTAS_IPC": "${:,.2f}", "VENT_PROM_IPC":"${:,.2f}", "PATENTES":"{:.0f}", "REGISTROS": "{:.0f}", "SECRETOS": "{:.0f}", "DERECHOS": "{:.0f}", "SECRETOS":"{:.0f}"}

# read_db_parque
def read_db(path_to_dbs,name_file):
  '''read db excel file''' # enhance with multiple type of files!
  dbs = Path(path_to_dbs)
  file_db = dbs / name_file
  return pd.read_excel(file_db)

# transform df
def preprocess_df(df):
  '''transform dataframe''' # -> enhance documentation

  df = df[['NOMBRE_EMPRENDIMIENTO', 'MES DEL ACOMPAÑAMIENTO REPORTADO',
       'VIGENCIA', 'MACROSECTOR','Institución emprendedor', '¿ESTA FORMALIZADA?',
       'NÚMERO DE EMPLEOS DIRECTOS GENERADOS AL MES',
       'TOTAL VENTAS MENSUALES EN COLOMBIA', 'NÚMERO DE PATENTES',
       'NÚMERO DE REGISTRO DE MARCA', 'NÚMERO DE DERECHOS DE AUTOR',
       'NÚMERO DE SECRETOS INDUSTRIALES']]

# Rename some cols
  df.rename(columns = {'NOMBRE_EMPRENDIMIENTO':'NOMBRE_EMPRENDIMIENTO',
                     'MES DEL ACOMPAÑAMIENTO REPORTADO':'MES',
                     'MACROSECTOR':'SECTOR',
                     'Institución emprendedor':'INSTITUCION',
                     '¿ESTA FORMALIZADA?':'FORMALIZACION',
                     'NÚMERO DE EMPLEOS DIRECTOS GENERADOS AL MES':'EMPLEOS',
                     'TOTAL VENTAS MENSUALES EN COLOMBIA':'VENTAS',
                     'NÚMERO DE PATENTES':'PATENTES',
                     'NÚMERO DE REGISTRO DE MARCA':'REGISTROS',
                     'NÚMERO DE DERECHOS DE AUTOR':'DERECHOS',
                     'NÚMERO DE SECRETOS INDUSTRIALES':'SECRETOS'
                     },inplace=True)

# Process some columns -> Month map equality

  month_map = {'ENERO': 1,
             'FEBRERO':2,
             'MARZO':3,
             'ABRIL' :4,
             'MAYO' :5,
             'JUNIO':6,
             'JULIO' : 7,
             'AGOSTO':8,
             'SEPTIEMBRE' :9,
             'OCTUBRE' :10,
             'NOVIEMBRE' :11,
             'DICIEMBRE':12,}

# Money mapper by inflation
# inflation: see: https://www.dineroeneltiempo.com/peso-colombiano/de-2021-a-valor-presente
  IPC= { 
  '2014' : 95.25,
  '2015'	: 100,
  '2016' :	107.51,
  '2017'	: 112.15,
  '2018' :	115.78,
  '2019' : 119.87,
  '2020' : 122.89,
  '2021'	: 127.19,
  '2022' : 138.73
  }

# formater. global variable

# Data transform
  df['MES'] = df['MES'].apply(lambda x:unidecode.unidecode(x.upper()))
  df['MES_DATE'] = df['MES'].map(month_map)
  df['DATE'] = pd.to_datetime(df.apply(lambda row: str(int(row.VIGENCIA)) +'-'+ str(int(row.MES_DATE)), axis=1),format='%Y-%m-%d')
  df['NOMBRE_EMPRENDIMIENTO'] = df['NOMBRE_EMPRENDIMIENTO'].apply(lambda x: unidecode.unidecode(x.upper()))
  df['FORMALIZACION'] = df['FORMALIZACION'].fillna('NO')
  df['FORMALIZACION'] = df['FORMALIZACION'].apply(lambda x: unidecode.unidecode(x.upper()))
  df['SECTOR'] = df['SECTOR'].apply(lambda x: unidecode.unidecode(str(x))).str.upper()
  df['INSTITUCION'] = df['INSTITUCION'].apply(lambda x: unidecode.unidecode(str(x))).str.upper()
  df['EMPLEOS'] = df['EMPLEOS'].fillna(0)
  df['VIGENCIA'] = df['DATE'].dt.year
  df['VENTAS_IPC'] = df.apply(lambda row: row.VENTAS*(IPC['2022']/IPC[str(row.VIGENCIA)]),axis=1)

  return df

# permanencia empresarialI
def permanencia_I(df):
  '''permanencia empresarial I''' # enhance with more parameters
  names = df['NOMBRE_EMPRENDIMIENTO'].unique()
  dflbp = pd.DataFrame()
  for i in names: 
    # min month
    a = df[(df['NOMBRE_EMPRENDIMIENTO'] == i)]['MES_DATE'].min()

    b = df[(df['NOMBRE_EMPRENDIMIENTO'] == i)]['MES_DATE'].max()

    #print(a,'-----',b)

   # row
    row_df = df[(df['NOMBRE_EMPRENDIMIENTO'] == i) & (df['MES_DATE'] == a)] #['FORMALIZACION']
  
  # EFIA
    efia =  row_df['FORMALIZACION'].values[0]

    row_df['EFIA'] =efia
 
    # EFFA
    effa = df[(df['NOMBRE_EMPRENDIMIENTO'] == i) & (df['MES_DATE'] == b)]['FORMALIZACION'].values[0] #['FORMALIZACION']
  
    row_df['EFFA'] = effa

    if (efia == "NO" and effa =="SI"):
      row_df["NOSI"] = 'SI'
    else:
      row_df["NOSI"] = 'NO'

#  if (efia == "SI" and effa =="SI"):
#   row_df["SISI"] = 'SI'
#  else:
#    row_df["SISI"] = 'NO'

    dflbp = dflbp.append(row_df)
  return dflbp 

# permanenciaII
# read db rues
# pathlib!
# remr = dbs / 'remr.xlsx'
# dfr = pd.read_excel(remr) 
dfr = pd.read_excel('registros_empresas_v2.xlsx')
# transform some cols
dfr['razon_social']= dfr['razon_social'].apply(lambda x: unidecode.unidecode(str(x).upper()))

# subset db parque
dataa = df['NOMBRE_EMPRENDIMIENTO'].unique()

# subset db rues
data = dfr['razon_social']

# find matchs
idp = 0
idr = 0

idpp = []
idrr = []

H = []

a = 0
aa = 0
for i in dataa:
  i = str(i).replace('S.A.S','').replace('SAS','').upper()
  for j in data:
    j = str(j).replace('S.A.S','').replace('SAS','').replace(' .','').upper()
    d = fuzz.ratio(i,j)
    if d >= 95:
      H.append((i,j))
      print(i,'--',j,'--',d)
      idpp.append(idp)
      idrr.append(idr)
      a+=1
    idr += 1
  idp += 1

# store findings
with open('H','wb') as fb:
  pickle.dump(H,fb) 

# compile two databases:dataframe parque hallazgos
#                       dataframe rues hallazgos
dfph = pd.DataFrame()
dfrh = pd.DataFrame()

for i in H:
  l = dfa[dfa['NOMBRE_EMPRENDIMIENTO'].str.contains(i[0]).fillna(False)]
  li = l.shape[0]
  r = dfr[dfr['razon_social'].str.contains(i[1]).fillna(False)]
  ri = r.shape[0]
  
  if li == 0:
    print(i[0],'----',i[1])
  else:
    dfph=dfph.append(l,ignore_index=True)

  if ri == 0:
    print(i[0],'----',i[1])
  else:
    dfrh=dfrh.append(r,ignore_index=True)

# save dbs
dfph.to_excel('dfph_v0.xlsx') 
dfrh.to_excel('dfrh_v0.xlsx')

