import cx_Oracle

import pandas as pd

import os

dsn_tns = cx_Oracle.makedsn('****', '****',
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
        
        
        
        
        
        
        

"""
sap = pd.read_sql(sap, con=conn)