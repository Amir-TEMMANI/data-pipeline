import cx_Oracle

import pandas as pd

import os

dsn_tns = cx_Oracle.makedsn('****', '****',
                            service_name='****')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
conn = cx_Oracle.connect(user=r'****', password='****',
                         dsn=dsn_tns)

impayes = """
select p.reference,p.agence , p.branche ,i.CATEGORIE  , p.assure , p.souscription  ,p.EXPIRATION , pp.nette  , r.MONTANT_POLICE,r.MONTANT_ECHEANCE,        
con.c , sum(v.ver) ,a.transaction  


from bi_police p , bi_assures i
, (select reference , sum(nette) as nette
from bi_police_prime group by reference  ) pp ,
(select reference , avg(MONTANT_POLICE) as  MONTANT_POLICE ,sum(MONTANT_ECHEANCE) as  MONTANT_ECHEANCE 
from BI_POLICE_ECHEANCIER_REGLEMENT group by reference ) r,
(select distinct substr(reference,1,18) as reference ,NUM_POLICE, NUM_ALIMENT , transaction from bi_police  where transaction in (19,20,30,49)) a,
( select reference , sum(versement) as ver from (
                                        SELECT
                                            reference,
                                            num,
                                           trunc(date_versement) AS date_versement,
                                            versement AS versement
                                        FROM
                                            bi_versement_new 
                                            where date_versement >=  TO_DATE('01/01/2006', 'dd/mm/yyyy')
                                    and trunc(date_versement) <= trunc(current_date)
                                    -- and trunc(date_versement) < to_date('01/01/2021','dd/mm/yyyy')


                                        UNION ALL

                                        SELECT
                                            "Reference"          AS reference,
                                            "Versement Numero"   AS num,
                                            trunc("Versement Date") AS date_versement,
                                            "Montant reglement par police" AS versement
                                        FROM
                                            bi_versements
                                        WHERE
                                            trunc("Versement Date") <> TO_DATE('01/10/2020', 'dd/mm/yyyy')
                                            and trunc("Versement Date") <= trunc(current_date)
                                              --and trunc("Versement Date") < to_date('01/01/2021','dd/mm/yyyy') 
                                              ) 
                                              group by reference ) v,
                                              (select distinct reference , '1' as c from BI_POLICE_ECHEANCIER_REGLEMENT
                                              where "Mode de Paiement" in ('Perte', 'Contentieux')
                                              ) con

where p.reference = pp.reference 
and p.reference = v.reference (+)
and p.reference = con.reference (+)
and p.reference = a.reference (+)
and p.NUM_POLICE = a.NUM_POLICE (+)
and p.NUM_ALIMENT = a.NUM_ALIMENT (+)
and p.reference = r.reference(+)
and p.PAIEMENT =3
and p.CODE_ASSURE = i.code 
and p.valider = 1
and p.document <> 6
and p.transaction not in (19,20,30,49)
group by p.reference,p.agence , p.branche,i.CATEGORIE ,p.assure , p.souscription  ,p.EXPIRATION , pp.nette  , r.MONTANT_POLICE,r.MONTANT_ECHEANCE,        
con.c  ,a.transaction  



"""
imp = pd.read_sql(impayes, con=conn)

reseau = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\reseau.gzip')
imp['AGENCE'] = imp['AGENCE'].astype(int)
imp['BRANCHE'] = imp['BRANCHE'].astype(str)


imp = imp.merge(reseau[['Agence', 'CODE AGENCE', 'direction', ]],
                left_on='AGENCE',
                right_on='Agence')

reseau2 = pd.read_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\branche.gzip')

reseau2['Code branche'] = reseau2['Code branche'].astype(str)


imp = imp.merge(reseau2[['Code branche', 'Branche1', 'Branche 2', ]],
                left_on='BRANCHE',
                right_on='Code branche')


imp.to_excel(r'\\qnap\dcg\DCG_data warehouse\impayes\impayes_police 2.xlsx')

try:
    os.remove(r'\\qnap\dcg\DCG_data warehouse\impayes\impayes_police.xlsx')
except:
    pass

os.rename(r'\\qnap\dcg\DCG_data warehouse\impayes\impayes_police 2.xlsx',
          r'\\qnap\dcg\DCG_data warehouse\impayes\impayes_police.xlsx')

