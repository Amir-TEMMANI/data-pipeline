import mysql.connector

import pandas as pd
import cx_Oracle
import os

dsn_tns = cx_Oracle.makedsn('1****', '****',
                            service_name='***')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
conn = cx_Oracle.connect(user=r'****', password='***',
                         dsn=dsn_tns)

mydb = mysql.connector.connect(
  host='***',
  user="****",
  password="@******"
)


commission = """
SELECT
    com.reference as reference ,
    com.agence as agence,
    com.class as class,
    com.branche as branche,

    sum(com.prime) as prime,
    sum(com.apport) as apport,
    sum(com.gestion) as gestion
FROM
    backoffice.view_police_commission com
    group by     com.reference,
    com.agence ,
    com.class,
    com.branche 

"""
production = """
select reference , souscription , transaction, assure,CODE_ASSURE  from bi_police where valider = 1
and document <> 6
and souscription >= to_date('01/10/2021','dd/mm/yyyy')
--and trunc(souscription,'y') =  trunc(current_date,'y')

"""



com = pd.read_sql(commission, con=mydb)
prod = pd.read_sql(production , con = conn)











com = com.merge(prod[['REFERENCE', 'SOUSCRIPTION', 'TRANSACTION','ASSURE','CODE_ASSURE' ]],
                left_on='reference',
                right_on='REFERENCE')



reseau = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\reseau.gzip')
com['agence'] = com['agence'].astype(int)
com['branche'] = com['branche'].astype(str)
print(reseau)

com = com.merge(reseau[['Agence', 'CODE AGENCE', 'direction','nom' ]],
                left_on='agence',
                right_on='Agence')

reseau2 = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\branche.gzip')

reseau2['Code branche'] = reseau2['Code branche'].astype(str)


com = com.merge(reseau2[['Code branche', 'Branche1', 'Branche 2', ]],
                left_on='branche',
                right_on='Code branche')

print(prod)
print(com.dtypes)
del com['REFERENCE']
del com['Agence']

parrain = """

SELECT view_filleul_0.code_filleul, view_filleul_0.code_parrain, view_filleul_0.created_at
FROM backoffice.view_filleul view_filleul_0



"""

com2= pd.read_sql(parrain, con=mydb)

com2 = com.merge(com2[['code_filleul', 'code_parrain', 'created_at', ]],
                left_on='CODE_ASSURE',
                right_on='code_filleul')

com2['nb']= 1

com2['cumul_filleul'] = com2.groupby(by=['code_filleul',])['nb'].transform('count')
com2['cumul_parrain'] = com2.groupby(by=['code_parrain',])['nb'].transform('count')


com2['nb_filleul'] = com2['nb']/com2['cumul_filleul']
com2['nb_parrain'] = com2['nb']/com2['cumul_parrain']














com2.to_csv(r'\\qnap\dcg\DCG_data warehouse\eden\eden_commissions 2.csv')

try:
    os.remove(r'\\qnap\dcg\DCG_data warehouse\eden\eden_commissions.csv')
except:
    pass

os.rename(r'\\qnap\dcg\DCG_data warehouse\eden\eden_commissions 2.csv',
          r'\\qnap\dcg\DCG_data warehouse\eden\eden_commissions.csv')
