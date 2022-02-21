import cx_Oracle

import pandas as pd

import os

dsn_tns = cx_Oracle.makedsn('****', '****',
                            service_name='***')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
conn = cx_Oracle.connect(user=r'****', password='****',
                         dsn=dsn_tns)






recouvrement  = """

select v.reference as reference  , trunc(p.souscription)  as souscription, p.assure as assure, trunc(v.date_versement) 
as date_versement , v.versement as versement ,p.agence as agence , p.branche as branche 
from

(select reference , DATE_VERSEMENT, VERSEMENT 
from  BI_VERSEMENT_NEW
where DATE_VERSEMENT >= to_date('01/01/2020','dd/mm/yyyy')
and DATE_VERSEMENT <= current_date
union all 
select "Reference", "Versement Date", "Montant reglement par police"
from BI_VERSEMENTS
where  trunc("Versement Date") <> to_date('01/10/2020','dd/mm/yyyy')
and trunc("Versement Date") <= trunc(current_date)) v, bi_police p,
(select distinct substr(reference,1,18) as reference ,NUM_POLICE, NUM_ALIMENT , transaction from bi_police  where transaction in (19,20,30,49)) a 



where
p.reference = v.reference 
and p.reference = a.reference (+)
and p.NUM_POLICE = a.NUM_POLICE (+)
and p.NUM_ALIMENT = a.NUM_ALIMENT (+)
and p.paiement = 3
and valider = 1
and document <> '6'

"""


rec = pd.read_sql(recouvrement, con=conn)

reseau = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\reseau.gzip')
rec['AGENCE'] = rec['AGENCE'].astype(int)
rec['BRANCHE'] = rec['BRANCHE'].astype(str)


rec = rec.merge(reseau[['Agence', 'CODE AGENCE', 'direction', ]],
                left_on='AGENCE',
                right_on='Agence')

reseau2 = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\branche.gzip')

reseau2['Code branche'] = reseau2['Code branche'].astype(str)


rec = rec.merge(reseau2[['Code branche', 'Branche1', 'Branche 2', ]],
                left_on='BRANCHE',
                right_on='Code branche')


rec.to_excel(r'\\qnap\dcg\DCG_data warehouse\versement\versement_police 2.xlsx')

try:
    os.remove(r'\\qnap\dcg\DCG_data warehouse\versement\versement_police.xlsx')
except:
    pass

os.rename(r'\\qnap\dcg\DCG_data warehouse\versement\versement_police 2.xlsx',
          r'\\qnap\dcg\DCG_data warehouse\versement\versement_police.xlsx')
