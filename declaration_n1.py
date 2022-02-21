import cx_Oracle

import pandas as pd

import os

dsn_tns = cx_Oracle.makedsn('***', '**',
                            service_name='**')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
conn = cx_Oracle.connect(user=r'***', password='*****',
                         dsn=dsn_tns)

declaration = """



select ref_sinistre as  , dec.agence  as agence, dec.branche  as branche, trunc(date_declaration) as date_declaration  , dec.LIBELLE_TYPE_SINISTRE as  LIBELLE_TYPE_SINISTRE,
    pp.garantie as garantie,pp.tiers as tiers,
    t.compagnie_tiers as compagnie_tiers,nb.CONTRE_X_COUNT as CONTRE_X_COUNT,
    sum(pp.variation) as variation

    from BI_DECLARATION_SINISTRE dec , BI_PROVISION_SINISTRE pp , BI_SINISTRE_TIERS t ,
    (select reference, region , count(tiers) as CONTRE_X_COUNT from BI_SINISTRE_TIERS group by reference, region) nb



    where dec.ref_sinistre = pp.reference 
    and pp.tiers = t.tiers 
    and pp.reference = t.reference 
        and t.region = nb.region 
            and t.reference = nb.reference 
 and trunc(date_provision) <= add_months(trunc(current_date), -12)
and t.agence <> '00000'

and trunc(dec.date_declaration, 'y') = trunc(add_months(current_date, -12), 'y')
and trunc(dec.date_declaration) <= add_months(trunc(current_date), -12)
    group by ref_sinistre , dec.agence, dec.branche , trunc(date_declaration) , dec.LIBELLE_TYPE_SINISTRE ,pp.tiers,
    pp.garantie ,
    t.compagnie_tiers ,nb.CONTRE_X_COUNT
"""

dec = pd.read_sql(declaration, con=conn)

reseau = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\reseau.gzip')
dec['AGENCE'] = dec['AGENCE'].astype(int)
dec['BRANCHE'] = dec['BRANCHE'].astype(str)
dec['GARANTIE'] = dec['GARANTIE'].fillna('0')
dec['GARANTIE'] = dec['GARANTIE'].astype(int)
dec['VARIATION'] = dec['VARIATION'].astype(float)

dec = dec.merge(reseau[['Agence', 'CODE AGENCE', 'direction', ]],
                left_on='AGENCE',
                right_on='Agence')

codi = pd.read_csv(r'\\qnap\dcg\DCG_data warehouse\codification\codification.csv')
#
dec = dec.merge(codi[['CODE', 'Nom rubrique', ]],
                left_on='GARANTIE',
                right_on='CODE',
                how='left')


###branche


reseau2 = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\branche.gzip')

reseau2['Code branche'] = reseau2['Code branche'].astype(str)
print(reseau.dtypes)
print(dec.dtypes)

dec = dec.merge(reseau2[['Code branche', 'Branche1', 'Branche 2', ]],
                left_on='BRANCHE',
                right_on='Code branche')

#

dec['GARANTIE'] = dec['GARANTIE'].astype(int)

del dec['Agence']






compagnie_tiers = pd.read_excel(r'\\qnap\dcg\DCG_data warehouse\config\Compagnie tiers.xlsx', dtype={0: 'str'})
compagnie_tiers['Code'] = compagnie_tiers['Code'].astype(object)

dec = dec.merge(compagnie_tiers[['Code', 'compagnie', ]],
                left_on='COMPAGNIE_TIERS',
                right_on='Code',
                how='left')

del dec['Code']

dec.to_csv(r'\\qnap\dcg\DCG_data warehouse\Declarations\declaration N-1 2.csv', decimal=',')

try:
    os.remove(r'\\qnap\dcg\DCG_data warehouse\Declarations\declaration N-1.csv')
except:
    pass

os.rename(r'\\qnap\dcg\DCG_data warehouse\Declarations\declaration N-1 2.csv',
          r'\\qnap\dcg\DCG_data warehouse\Declarations\declaration N-1.csv')