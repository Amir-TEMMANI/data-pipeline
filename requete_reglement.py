import cx_Oracle

import pandas as pd

import os



dsn_tns = cx_Oracle.makedsn('****', '***',
                                service_name='***')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
conn = cx_Oracle.connect(user=r'***', password='****',
                             dsn=dsn_tns)


reglement = """
select r.reference as reference , r.agence as agence , r.type_sinistre as type_sinistre  ,
 r.date_reglement  as date_reglement ,r.tiers  as tiers, r.garantie  as garantie  ,r.nom_rubrique as nom_rubrique,trunc(dec.date_declaration) as date_declaration,
  sum(r.valeur) as valeur , COMPAGNIE_TIERS
from bi_reglement_sinistre r , BI_SINISTRE_TIERS t , BI_SINISTRE_DECLARATIONS dec
where
 trunc(date_reglement,'y') >= trunc(add_months(current_date,-12),'y')
and t.reference = r.reference 
and r.reference = dec.ref_sinistre 
and r.tiers = t.tiers
and r.nom_rubrique  not in ('Recours Aboutis','TVA')

 group by r.reference, r.agence , r.type_sinistre  , r.date_reglement ,r.tiers , r.garantie ,r.nom_rubrique , COMPAGNIE_TIERS ,trunc(dec.date_declaration) 

"""
reg = pd.read_sql(reglement, con=conn)

reseau = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\reseau.gzip')
reg['AGENCE'] = reg['AGENCE'].astype(int)
reg['GARANTIE'] =reg['GARANTIE'].fillna('0')
reg['GARANTIE'] = reg['GARANTIE'].astype(int)


reg = reg.merge(reseau[['Agence','CODE AGENCE', 'direction', ]],
                        left_on='AGENCE',
                        right_on='Agence')

codi= pd.read_csv(r'\\qnap\dcg\DCG_data warehouse\codification\codification.csv')
#
reg = reg.merge(codi[['CODE','Nom rubrique', ]],
                        left_on='GARANTIE',
                        right_on='CODE',
                how= 'left')
del reg['Agence']

compagnie_tiers = pd.read_excel(r'\\qnap\dcg\DCG_data warehouse\config\Compagnie tiers.xlsx', dtype={0:'str'})
compagnie_tiers['Code'] = compagnie_tiers['Code'].astype(object)


reg = reg.merge(compagnie_tiers[['Code','compagnie', ]],
                        left_on='COMPAGNIE_TIERS',
                        right_on='Code',
                how= 'left')

del reg['Code']



reg.to_csv(r'\\qnap\dcg\DCG_data warehouse\Reglements\reglement2.csv', decimal=',')



try:
    os.remove(r'\\qnap\dcg\DCG_data warehouse\Reglements\reglement.csv')
except:
    pass

os.rename(r'\\qnap\dcg\DCG_data warehouse\Reglements\reglement2.csv',r'\\qnap\dcg\DCG_data warehouse\Reglements\reglement.csv')
