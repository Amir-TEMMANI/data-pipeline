import mysql.connector

import pandas as pd

import os

# dsn_tns = cx_Oracle.makedsn('**', '**',
#                             service_name='**')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
# conn = cx_Oracle.connect(user=r'**', password='@****',
#                          dsn=dsn_tns)

mydb = mysql.connector.connect(
  host='**',
  user="**",
  password="@****"
)


cotation= """
SELECT c.num_cotation,
 c.branche, c.agence, c.etat_cota,
  c.genere, c.code_contrat, c.reference_iris,c.date_demande
FROM backoffice.view_cotation c
where c.agence <> 1
and branche is not null



"""

cot = pd.read_sql(cotation, con=mydb)
print(cot)

reseau = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\reseau.gzip')
cot['agence'] = cot['agence'].astype(int)
# cot['BRANCHE'] = cot['BRANCHE'].astype(str)


cot = cot.merge(reseau[['Agence', 'CODE AGENCE', 'direction', ]],
                left_on='agence',
                right_on='Agence')

reseau2 = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\branche.gzip')

reseau2['Code branche'] = reseau2['Code branche'].astype(str)


cot = cot.merge(reseau2[['Code branche', 'Branche1', 'Branche 2', ]],
                left_on='branche',
                right_on='Code branche')


cot.to_excel(r'\\qnap\dcg\DCG_data warehouse\cotations\cotation 2.xlsx')

try:
    os.remove(r'\\qnap\dcg\DCG_data warehouse\cotations\cotation.xlsx')
except:
    pass

os.rename(r'\\qnap\dcg\DCG_data warehouse\cotations\cotation 2.xlsx',
          r'\\qnap\dcg\DCG_data warehouse\cotations\cotation.xlsx')
