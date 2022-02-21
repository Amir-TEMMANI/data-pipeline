import cx_Oracle
import datetime as dt
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os
import shutil

dsn_tns = cx_Oracle.makedsn('****', '****',
                                service_name='****')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
conn = cx_Oracle.connect(user=r'****', password='****',
                             dsn=dsn_tns)

sap=  """ select reference, tiers, datee, RUBRIQUE, MONTANT,branche,TYPE_SINISTRE, agence , ANNEE_SINISTRE , COMPAGNIE_TIERS ,sum(MONTANT) OVER(
                                PARTITION BY reference , tiers, rubrique
                                ORDER BY
                                    DATEE
                            ) AS cumul , ROW_NUMBER() OVER(
                                PARTITION BY reference , tiers, rubrique
                                ORDER BY
                                    DATEE
                            ) AS cumul_nombre

from (

select dt.reference as reference , dt.tiers as tiers , dt.datee as datee,rub.garantie as RUBRIQUE, sum(pp) as pp,sum(ph) as ph ,sum(rp) as rp ,sum(rh) as rh, sum(solde)as solde  , t.BRANCHE as branche,
        t.TYPE_SINISTRE as TYPE_SINISTRE, t.agence as agence , t.ANNEE_SINISTRE as ANNEE_SINISTRE, t.COMPAGNIE_TIERS as COMPAGNIE_TIERS ,(sum(pp) +sum(ph) -sum(rp) -sum(rh) - sum(solde))as MONTANT
        
        
        
        
        from (
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
        union all 
        (select reference,region , tiers , statut_date as datee,0,0,0,0,sum(solde)
        from (
        select p.reference as reference ,p.region as region ,p.tiers as tiers , p.variation , r.valeur , s.LIB_STATUT, s.statut_date as  statut_date,(p.variation - r.valeur) as solde   from
        
        (select reference,region ,tiers ,  sum(variation) as variation
        from BI_PROVISION_SINISTRE 
        where RUBRIQUE = 1
        group by  reference,region ,tiers ) p,
        (select reference,region , tiers , sum(valeur)  as valeur 
        from BI_reglement_SINISTRE 
        where RUBRIQUE =1
        and nom_RUBRIQUE <> 'Franchise'
        group by  reference,region ,tiers ) r,
        
        (select  s.reference,s.region, o.ORDRE, s.LIB_STATUT , trunc(s.STATUT_DATE) as statut_date
         
        from BI_SINISTRE_STATUTS s ,  ( select reference ,region, max (ORDRE) as ORDRE from  BI_SINISTRE_STATUTS     
                                    group by reference,region  ) o
        where
             s.reference = o.reference
             and s.region = o.region
            and  s.ORDRE = o.ORDRE
            --care
            and s.LIB_STATUT in ('Fermer','Classé') ) s
            
        where p.reference = r.reference (+)
        and p.region = r.region(+)
        and  p.tiers = r.tiers (+)
        and p.reference = s.reference 
        and p.reference = s.region
        and  p.variation - r.valeur <> 0 
        )
        group by reference,region , tiers , STATUT_DATE 
        )
        
        
        
        
        
        
        ) dt, 
        (select reference,region , tiers , garantie from bi_provision_sinistre
        where rubrique = 1
        group by 
        reference ,region, tiers , garantie) rub,
        BI_SINISTRE_TIERS t
        
        where  dt.reference = rub.reference (+)
        and dt.tiers =rub.tiers (+)
        and dt.reference = t.reference 
        and dt.tiers = t.tiers 
        group by  dt.reference , dt.tiers , dt.datee, rub.garantie,  t.BRANCHE ,
        t.TYPE_SINISTRE, t.agence , t.ANNEE_SINISTRE, t.COMPAGNIE_TIERS
        order by dt.datee
        ) 
        
        
"""

df_sap= pd.read_sql(sap, con=conn)
# df_sap= pd.read_csv(r'C:\Users\matemmani\PycharmProjects\import sap\sap.csv', decimal=',')

# df_sap['PP'] =df_sap['PP'].fillna(0)
# df_sap['PH'] =df_sap['PH'].fillna(0)
# df_sap['RP'] =df_sap['RP'].fillna(0)
# df_sap['RH'] =df_sap['RH'].fillna(0)
# df_sap['SOLDE'] =df_sap['SOLDE'].fillna(0)
#
# df_sap['MONTANT'] = df_sap['PP'] +df_sap['PH'] -df_sap['RP']-df_sap['RH']-df_sap['SOLDE']

sap1 = df_sap

df_reseau = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\reseau.gzip')


sap1['agencenum'] = sap1['AGENCE'].astype(int)
df_reseau['agencenum2'] = df_reseau['Agence'].astype(int)

sap1= sap1.merge(df_reseau[['Agence', 'STATUT', 'CODE AGENCE', 'direction', 'nom', 'Agence 3', ]],left_on='agencenum', right_on=df_reseau.agencenum2)

sap1 = sap1[['REFERENCE','TIERS', 'DATEE', 'RUBRIQUE', 'MONTANT','ANNEE_SINISTRE','COMPAGNIE_TIERS','CUMUL','CUMUL_NOMBRE','CODE AGENCE','TYPE_SINISTRE','direction','nom','Agence 3']]
print(sap1.dtypes)

print(sap1.MONTANT.sum())

np.where(sap1['CUMUL_NOMBRE']==1 , 1 , np.where((sap1['MONTANT']<= 1000 ) & (sap1['CUMUL']<= 1000 ) , -1,0      ) )




sap1.to_csv(r'C:\Users\matemmani\PycharmProjects\import sap\sap1.csv', decimal=',')
try:
    os.remove(r'C:\Users\matemmani\PycharmProjects\import sap\sap.csv')
except:
    pass
os.rename(r'C:\Users\matemmani\PycharmProjects\import sap\sap1.csv',r'C:\Users\matemmani\PycharmProjects\import sap\sap.csv')
shutil.copyfile(r'C:\Users\matemmani\PycharmProjects\import sap\sap.csv', r'\\qnap\dcg\DCG_data warehouse\sap\sap.csv')




DRC_OUEST = sap1.loc[sap1.direction == 'DRC OUEST']
DRC_OUEST.to_csv(r'C:\Users\matemmani\PycharmProjects\import sap\DRC OUEST1.csv', decimal=',')
try:
    os.remove(r'C:\Users\matemmani\PycharmProjects\import sap\DRC OUEST.csv')
except:
    pass
os.rename(r'C:\Users\matemmani\PycharmProjects\import sap\DRC OUEST1.csv',r'C:\Users\matemmani\PycharmProjects\import sap\DRC OUEST.csv')



DRC_EST = sap1.loc[sap1.direction == 'DRC EST']
DRC_EST.to_csv(r'C:\Users\matemmani\PycharmProjects\import sap\DRC EST1.csv', decimal=',')
try:
    os.remove(r'C:\Users\matemmani\PycharmProjects\import sap\DRC EST.csv')
except:
    pass
os.rename(r'C:\Users\matemmani\PycharmProjects\import sap\DRC EST1.csv',r'C:\Users\matemmani\PycharmProjects\import sap\DRC EST.csv')


DR_BLIDA = sap1.loc[sap1.direction == 'DR BLIDA']
DR_BLIDA.to_csv(r'C:\Users\matemmani\PycharmProjects\import sap\DR BLIDA1.csv', decimal=',')
try:
    os.remove(r'C:\Users\matemmani\PycharmProjects\import sap\DR BLIDA.csv')
except:
    pass
os.rename(r'C:\Users\matemmani\PycharmProjects\import sap\DR BLIDA1.csv',r'C:\Users\matemmani\PycharmProjects\import sap\DR BLIDA.csv')



DRA = sap1.loc[sap1.direction == 'DRA']
DRA.to_csv(r'C:\Users\matemmani\PycharmProjects\import sap\DRA1.csv', decimal=',')
try:
    os.remove(r'C:\Users\matemmani\PycharmProjects\import sap\DRA.csv')
except:
    pass
os.rename(r'C:\Users\matemmani\PycharmProjects\import sap\DRA1.csv',r'C:\Users\matemmani\PycharmProjects\import sap\DRA.csv')



DRO = sap1.loc[sap1.direction == 'DRO']
DRO.to_csv(r'C:\Users\matemmani\PycharmProjects\import sap\DRO1.csv', decimal=',')
try:
    os.remove(r'C:\Users\matemmani\PycharmProjects\import sap\DRO.csv')
except:
    pass
os.rename(r'C:\Users\matemmani\PycharmProjects\import sap\DRO1.csv',r'C:\Users\matemmani\PycharmProjects\import sap\DRO.csv')



DRS = sap1.loc[sap1.direction == 'DRS']
DRS.to_csv(r'C:\Users\matemmani\PycharmProjects\import sap\DRS1.csv', decimal=',')
try:
    os.remove(r'C:\Users\matemmani\PycharmProjects\import sap\DRS.csv')
except:
    pass
os.rename(r'C:\Users\matemmani\PycharmProjects\import sap\DRS1.csv',r'C:\Users\matemmani\PycharmProjects\import sap\DRS.csv')



DRT = sap1.loc[sap1.direction == 'DRT']
DRT.to_csv(r'C:\Users\matemmani\PycharmProjects\import sap\DRT1.csv', decimal=',')
try:
    os.remove(r'C:\Users\matemmani\PycharmProjects\import sap\DRT.csv')
except:
    pass

os.rename(r'C:\Users\matemmani\PycharmProjects\import sap\DRT1.csv',r'C:\Users\matemmani\PycharmProjects\import sap\DRT.csv')

