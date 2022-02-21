import cx_Oracle

import pandas as pd

import os

dsn_tns = cx_Oracle.makedsn('****', '***',
                            service_name='***')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
conn = cx_Oracle.connect(user=r'****', password='***',
                         dsn=dsn_tns)

prod_annee = """

select p.agence ,EXTRACT(year FROM trunc(souscription,'y')) as annee , sum(pp.nette) as nette  


from bi_police p , bi_police_prime pp
where p.valider = 1
 and p.document <> 6

 and trunc(souscription,'y') >= trunc(add_months(current_date, - 48),'y')
 and code not in (2,3,4,5,6)
 and p.reference = pp.reference 
  group by p.agence , trunc(souscription,'y')
"""


prod_annee = pd.read_sql(prod_annee, con=conn)

reseau = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\reseau.gzip')
prod_annee['AGENCE'] = prod_annee['AGENCE'].astype(int)
prod_annee['ANNEE'] = prod_annee['ANNEE'].astype(str)
print(prod_annee)
prod_annee = prod_annee.pivot(index='AGENCE', columns='ANNEE', values='NETTE').reset_index()
print(prod_annee)


# prod_annee.columns[2].replace('NaN', '0')
# prod_annee.columns[3].replace('NaN', '0')
# prod_annee.columns[4].replace('NaN', '0')
# prod_annee.columns[5].replace('NaN', '0')
# prod_annee.columns[6].replace('NaN', '0')
#
#
# prod_annee.columns[2]= prod_annee.columns[2].astype(float)
# prod_annee.columns[3]= prod_annee.columns[3].astype(float)
# prod_annee.columns[4]= prod_annee.columns[4].astype(float)
# prod_annee.columns[5]= prod_annee.columns[5].astype(float)
# prod_annee.columns[6]= prod_annee.columns[6].astype(float)
#


# # prod_annee.columns[1].fillna(0, inplace=True)
# prod_annee.columns[2].fillna(0, inplace=True)
# prod_annee.columns[3].fillna(0, inplace=True)
# prod_annee.columns[4].fillna(0, inplace=True)
# prod_annee.columns[5].fillna(0, inplace=True)
prod_annee = prod_annee.merge(reseau[['Agence', 'CODE AGENCE', 'direction', ]],
                left_on='AGENCE',
                right_on='Agence')


prod_annee.iloc[:,2].fillna(0, inplace=True)
prod_annee.iloc[:,3].fillna(0, inplace=True)
prod_annee.iloc[:,4].fillna(0, inplace=True)
prod_annee.iloc[:,5].fillna(0, inplace=True)
prod_annee.iloc[:,1].fillna(0, inplace=True)
print(prod_annee.iloc[:, 1].sum())
# prod_annee['N-1'] = (prod_annee.iloc[:,3]-prod_annee.iloc[:,4])/prod_annee.iloc[:,3]
# prod_annee['N-2'] = (prod_annee.iloc[:,2]-prod_annee.iloc[:,3])/prod_annee.iloc[:,2]
# prod_annee['N-3'] = (prod_annee.iloc[:,1]-prod_annee.iloc[:,2])/prod_annee.iloc[:,1]


print(prod_annee)
prod_annee.to_parquet(r'C:\Users\matemmani\PycharmProjects\import production\prod_annee.gzip',
                      compression='gzip')