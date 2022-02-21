
import cx_Oracle
import numpy as np
import pandas as pd
import logging

import os


logging.basicConfig(filename="C:\\Users\\matemmani\\PycharmProjects\\scripte\\sap.log" , level = logging.DEBUG)
dsn_tns = cx_Oracle.makedsn('****', '***',
                            service_name='***')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
conn = cx_Oracle.connect(user=r'***', password='***',
                         dsn=dsn_tns)

sap = """
        (select reference,region , tiers , trunc(date_provision) as datee, sum(variation) as pp,0 as ph,0 as rp,0 as rh, 0 as solde 
        from BI_DATA.bi_provision_sinistre
        where trunc(date_provision)<=trunc(current_date) and rubrique = 1  and agence <>'00000'
        group by  reference,region , tiers , trunc(date_provision)
        ) 
        union all 

        (select reference,region , tiers , trunc(date_provision) as datee, 0, sum(variation) as valeur,0,0,0
        from BI_DATA.bi_provision_sinistre
        where trunc(date_provision)<=trunc(current_date) and rubrique = 2 and nom_rubrique = 'Honoraire' and agence <>'00000'
        group by  reference,region , tiers , trunc(date_provision)
        ) 

        union all 

        (select reference,region , tiers , trunc(date_reglement) as datee, 0,0, sum(valeur) as valeur,0,0
        from BI_DATA.bi_reglement_sinistre
        where trunc(date_reglement)<=trunc(current_date) and rubrique = 1 and nom_rubrique = 'Principal' and agence <>'00000'
        group by  reference,region , tiers , trunc(date_reglement)
        ) 

        union all 

        (select reference,region , tiers , trunc(date_reglement) as datee,0,  0,0, sum(valeur) as valeur,0
        from BI_DATA.bi_reglement_sinistre
        where trunc(date_reglement)<=trunc(current_date)  and rubrique = 2 and nom_rubrique = 'Honoraire'   and  agence <>'00000'
        group by  reference ,region, tiers , trunc(date_reglement)
        )










"""
sap = pd.read_sql(sap, con=conn)
logger = logging.getLogger()
logger.info('test1')
solde = """

select p.reference  as reference ,p.region as region  , p.tiers as tiers , (p.variation - r.valeur )as solde 
from
(select reference , region , tiers  ,sum(variation) as variation from bi_provision_sinistre   where RUBRIQUE  in (1,2) group by reference , region , tiers   ) p,
(select reference , region , tiers  ,sum(valeur) as  valeur from bi_reglement_sinistre  where NOM_RUBRIQUE  in ('Principal' ,'Honoraire')  group by reference , region , tiers  ) r
where p.reference = r.reference (+)
and p.region = r.region (+)
and p.tiers = r.tiers (+)


and ( p.variation - r.valeur ) >10 

"""
solde = pd.read_sql(solde, con=conn)

st = """
select  s.reference as reference ,s.region as region, o.ORDRE, s.LIB_STATUT , trunc(s.STATUT_DATE) as statut_date

        from BI_SINISTRE_STATUTS s ,  ( select  distinct reference ,region, max (ORDRE) as ORDRE from  BI_SINISTRE_STATUTS     
                                    group by reference,region  ) o
        where
             s.reference = o.reference
             and s.region = o.region
            and  s.ORDRE = o.ORDRE
            --care
            and s.LIB_STATUT in ('Fermer','Classé') 



"""
st = pd.read_sql(st, con=conn)

solde = solde.merge(st[['REFERENCE','STATUT_DATE', 'LIB_STATUT', ]],
                        left_on='REFERENCE',
                        right_on='REFERENCE')


solde['PP'] =0
solde['PH'] =0
solde['RP'] =0
solde['RH'] =0
logger.info('test2')
solde['DATEE'] = solde['STATUT_DATE']
solde = solde [['REFERENCE','REGION','TIERS','DATEE','PP','PH','RP','RH','SOLDE',]]

garanties = """

select reference ,tiers  ,max(garantie) as garantie
from bi_provision_sinistre 
group by reference ,tiers  


"""
logger.info('test3')
garanties = pd.read_sql(garanties, con=conn)


sap['DATEE'] =sap['DATEE'].astype(object)
sap['SOLDE'] =sap['SOLDE'].astype(float)

sap2 = pd.concat([sap ,solde] )


sap2 = sap2.merge(garanties[['REFERENCE','TIERS', 'GARANTIE', ]],
                        left_on=['REFERENCE','TIERS'],
                        right_on=['REFERENCE','TIERS'])


sap2['MONTANT'] = sap2['PP'] +sap2['PH']-sap2['RP']-sap2['RH']-sap2['SOLDE']
sap2 = sap2[['REFERENCE', 'REGION', 'TIERS','DATEE','GARANTIE','MONTANT']]


print(sap2['MONTANT'].sum())
tiers = """

select reference ,agence,branche,tiers, ANNEE_SINISTRE, COMPAGNIE_TIERS, TYPE_SINISTRE from BI_SINISTRE_TIERS

"""
logger.info('test4')
tiers =  pd.read_sql(tiers, con=conn)


sap2 = sap2.merge(tiers[['REFERENCE','AGENCE','BRANCHE','TIERS','ANNEE_SINISTRE', 'COMPAGNIE_TIERS','TYPE_SINISTRE' ]],
                        left_on=['REFERENCE','TIERS'],
                        right_on=['REFERENCE','TIERS'])

sap2['DATEE'] =  pd.to_datetime(sap2['DATEE'])
sap2 = sap2.sort_values(["DATEE"], ascending=True)

sap2['CUMUL'] = sap2.groupby(by=['REFERENCE','TIERS','GARANTIE'])['MONTANT'].cumsum()

reseau = pd.read_parquet(r'\\qnap\dcg\DCG_data warehouse\config\reseau.gzip')
sap2['AGENCE'] = sap2['AGENCE'].astype(int)

sap2 = sap2.merge(reseau[['Agence','CODE AGENCE', 'direction', ]],
                        left_on='AGENCE',
                        right_on='Agence')
print(sap2['DATEE'])
sap2['GARANTIE'] = sap2['GARANTIE'].astype(int)
sap2['BRANCHE'] = sap2['BRANCHE'].astype(int)

branche = pd.read_parquet(r'\\qnap\dcg\DCG_data warehouse\config\branche.gzip')
codi= pd.read_csv(r'\\qnap\dcg\DCG_data warehouse\codification\codification.csv')
print(sap2['DATEE'])
sap2 = sap2.merge(branche[['Code branche', 'Branche1', 'Branche 2', ]],  left_on='BRANCHE', right_on='Code branche')
sap2 = sap2.merge(codi[['CODE','Nom rubrique', ]],      left_on='GARANTIE',     right_on='CODE')
print(sap2.dtypes)

sap2 = sap2.sort_values(["DATEE"], ascending=True)
print(sap2['DATEE'])
sap2['nb'] = 1
sap2['nb_c'] = sap2.groupby(by=['REFERENCE','TIERS','GARANTIE'])['nb'].cumsum()


sap2['NOMBRE'] = np.where((sap2['CUMUL'] >= 998) & (sap2['CUMUL'] - sap2['MONTANT'] <= 998), 1,

                          np.where((sap2['CUMUL'] <= 998) & (sap2['CUMUL'] - sap2['MONTANT'] >= 998), -1, 0))

print(sap2['NOMBRE'].sum())
logger.info('testcsv deb')
compagnie = pd.read_csv(r'\\qnap\dcg\DCG_data warehouse\config\Compagnie tiers.csv')
sap2['COMPAGNIE_TIERS'] = sap2['COMPAGNIE_TIERS'].fillna(0).astype(int)
logger.info('testcsv')
print(compagnie)
compagnie['Code'] = compagnie['Code'].astype(int)
compagnie['Code'] = compagnie['Code'].astype(object)
print(compagnie)
sap2 = sap2.merge(compagnie[['Code','compagnie']],
                        left_on=['COMPAGNIE_TIERS'],
                        right_on=['Code',],how = 'left' )
print(sap2)
logger.info('test6')
del sap2['Code']
del sap2['Agence']

print(sap2['MONTANT'].sum())
logger.info('test7')
sap2.to_csv(r'\\qnap\dcg\DCG_data warehouse\sap\sap_global2.csv')


logger.info('test9')
try:
    os.remove(r'\\qnap\dcg\DCG_data warehouse\sap\sap_global.csv')
except:
    pass

os.rename(r'\\qnap\dcg\DCG_data warehouse\sap\sap_global2.csv',r'\\qnap\dcg\DCG_data warehouse\sap\sap_global.csv')







