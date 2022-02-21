import cx_Oracle

import pandas as pd

import os



dsn_tns = cx_Oracle.makedsn('****', '****',
                                service_name='****')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
conn = cx_Oracle.connect(user=r'****', password='*****',
                             dsn=dsn_tns)


recours = """

SELECT * from(select
   
    
reference,agence,TYPE_SINISTRE , nom  , trunc(date_recours)  as date_recours,NOM_RUBRIQUE, valeur
FROM
BI_RECOURS_SINSITRE
    
    WHERE
trunc(date_recours,'y') = trunc(current_date,'y') 
   
    ) t
    
    
   
pivot (
   sum(t.valeur)
for Nom_rubrique in ( 'Rcours Aboutis Compagnie' compagnie, 'Rcours Aboutis Assuré' assure, 'Montant Réclamé' reclame )
)

"""
rec = pd.read_sql(recours, con=conn)

reseau = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\reseau.gzip')
rec['AGENCE'] = rec['AGENCE'].astype(int)



rec = rec.merge(reseau[['Agence','CODE AGENCE', 'direction', ]],
                        left_on='AGENCE',
                        right_on='Agence')

# codi= pd.read_csv(r'\\qnap\dcg\DCG_data warehouse\codification\codification.csv')
# #
# reg = reg.merge(codi[['CODE','Nom rubrique', ]],
#                         left_on='GARANTIE',
#                         right_on='CODE',
#                 how= 'left')
del rec['Agence']

#
#
rec.to_csv(r'\\qnap\dcg\DCG_data warehouse\recours\recours2.csv', decimal=',')
#
#
#
try:
    os.remove(r'\\qnap\dcg\DCG_data warehouse\recours\recours.csv')
except:
    pass

os.rename(r'\\qnap\dcg\DCG_data warehouse\recours\recours2.csv',r'\\qnap\dcg\DCG_data warehouse\recours\recours.csv')
print(rec)