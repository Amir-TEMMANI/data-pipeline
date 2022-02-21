import cx_Oracle
import datetime as dt
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


dsn_tns = cx_Oracle.makedsn('****', '****',
                                service_name='devdb')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
conn = cx_Oracle.connect(user=r'**', password='**',
                             dsn=dsn_tns)



imp = """ select t.*   from



(SELECT
    reg.*,
    p.assure,
    p.souscription, 
    i.transaction,

    p.num_aliment,
    p.agence ,
    mla.mla_etat,
    count(reg.num_echeance) OVER(
                                        PARTITION BY reg.reference, reg.num_echeance

                                    ) numero
FROM
    (
        SELECT
            r.reference           reference,
            r.num_echeance        num_echeance,
            trunc(r.date_souscription)   date_souscription,
            r.mode_paiement       mode_paiement,
            r.date_echeance       date_echeance,
            r.montant_echeance    montant_echeance,
            sum(r.reglement_echeancier) reglement_echeancier,
            r.date_reglement date_reglement,

            con.etat              AS etat_police,
            ( r.montant_echeance - MAX(r.reglement_echeancier) ) AS impayées
        FROM
            (

-----------Debut cumul -----------------
                SELECT
                    reference,
                    num_echeance,
                    date_souscription,
                    mode_paiement,
                    date_echeance,
                    montant_echeance,
                    echeance_an,
                    num,
                    cumul_max,
                    date_versement,
                    versement,
                    cumul_ver,
                    num3,
                     case --1
                    when ( ( (cumul_ver - versement) >= (cumul_max - montant_echeance) ) and  (cumul_ver ) <= (cumul_max ) )  and (cumul_ver - versement) <= (cumul_max  ) 
                    then  versement

                    when ( (cumul_ver - versement) >= (cumul_max - montant_echeance ) and  (cumul_ver ) >= (cumul_max )    and (cumul_ver - versement) <= (cumul_max  )      ) 
                    then  cumul_max - (cumul_ver - versement)  

                    when ( (cumul_ver - versement) <= (cumul_max - montant_echeance ) and  (cumul_ver ) >= (cumul_max )  ) 
                    then  montant_echeance

                    when ( (cumul_ver - versement) <= (cumul_max - montant_echeance ) and  (cumul_ver ) >= (cumul_max - montant_echeance )  and  (cumul_ver  <= cumul_max )  and (cumul_ver - versement) <= (cumul_max  )    ) 
                   then  cumul_ver - (cumul_max - montant_echeance )  
                        ELSE
                            0 --date_versement
                    END AS reglement_echeancier,
                     case --1


                    when ( ( (cumul_ver - versement) >= (cumul_max - montant_echeance) ) and  (cumul_ver ) <= (cumul_max ) )  and (cumul_ver - versement) <= (cumul_max  ) 
                    then  date_versement

                    when ( (cumul_ver - versement) >= (cumul_max - montant_echeance ) and  (cumul_ver ) >= (cumul_max )    and (cumul_ver - versement) <= (cumul_max  )      ) 
                    then  date_versement  

                    when ( (cumul_ver - versement) <= (cumul_max - montant_echeance ) and  (cumul_ver ) >= (cumul_max )  ) 
                    then  date_versement

                    when ( (cumul_ver - versement) <= (cumul_max - montant_echeance ) and  (cumul_ver ) >= (cumul_max - montant_echeance )  and  (cumul_ver  <= cumul_max )  and (cumul_ver - versement) <= (cumul_max  )    ) 
                   then  date_versement 
                        ELSE
                           null
                    END AS date_reglement 




                FROM
                    (
                        SELECT
                            reference,
                            num_echeance,
                            date_souscription,
                            mode_paiement,
                            date_echeance,
                            montant_echeance,
                            echeance_an,
                            num,
                            cumul_max,
                            date_versement,
                            versement,
                            num3,

                            SUM(versement) OVER(
                                PARTITION BY reference, num_echeance, date_souscription, mode_paiement, date_echeance,
                                             montant_echeance, cumul_max
                                ORDER BY
                                    num
                            ) AS cumul_ver
                        FROM
                            (
                                SELECT
                                    r.reference            AS reference,
                                    r.num_echeance         AS num_echeance,
                                    r.date_souscription    AS date_souscription,
                                    r."Mode de Paiement"   AS mode_paiement,
                                    r.date_echeance        AS date_echeance,
                                    abs(r.montant_echeance)     AS montant_echeance,
                                    r.echeance_an          AS echeance_an,

                                    ROW_NUMBER() OVER(
                                        PARTITION BY r.reference, v.num
                                        ORDER BY
                                            r.num_echeance
                                    ) num3,
                                    SUM(r.montant_echeance) OVER(
                                        PARTITION BY r.reference, v.num
                                        ORDER BY
                                            r.num_echeance
                                    ) AS cumul_max,
                                    v.num                  AS num,
                                    v.date_versement       AS date_versement,
                                    v.versement            AS versement
                                FROM
                                    (
                                        SELECT
                                            c.reference            AS reference,
                                            c.num_echeance         AS num_echeance,
                                            c.date_souscription    AS date_souscription,
                                            c."Mode de Paiement"   AS "Mode de Paiement",
                                            c.date_echeance        AS date_echeance,
                                            c.montant_echeance     AS montant_echeance,
                                            b.echeance_an          AS echeance_an,
                                            c.num3 as num3
                                        FROM
                                            (
                                                SELECT
                                                    reference,
                                                    num_echeance,
                                                    date_souscription,
                                                    "Mode de Paiement",
                                                    date_echeance,
                                                    abs(montant_echeance) as montant_echeance ,
                                                    ROW_NUMBER() OVER(
                                                        PARTITION BY reference
                                                        ORDER BY
                                                            num_echeance
                                                    ) num3
                                                FROM
                                                    bi_police_echeancier_reglement

                                                   where montant_echeance >=0

                                            ) c,
                                            (
                                                SELECT
                                                    reference,
                                                    num_echeance       AS num_2,
                                                    abs(montant_echeance)   AS echeance_an,
                                                    ROW_NUMBER() OVER(
                                                        PARTITION BY reference
                                                        ORDER BY
                                                            num_echeance
                                                    )+1 num3
                                                FROM
                                                    bi_police_echeancier_reglement

                                                     where montant_echeance >=0
                                            ) b
                                        WHERE
                                            c.reference = b.reference (+)
                                            AND c.num3 = b.num3 (+)
                                            and  c.montant_echeance >=0
--group by c.reference ,c.NUM_ECHEANCE, c.date_souscription ,  c."Mode de Paiement",c.DATE_ECHEANCE,  b.echeance_an
--having  sum(MONTANT_ECHEANCE) >=0

                                    ) r,




--------------------
                                    (
                                        SELECT
                                            reference,
                                            num ,
                                           trunc(date_versement) AS date_versement,
                                            versement AS versement
                                            ,'ver1' as type
                                        FROM
                                            bi_versement_new 
                                            where date_versement >=  TO_DATE('01/01/2006', 'dd/mm/yyyy')
                                    and trunc(date_versement) <= trunc(current_date)
                   --   and   reference = '05619 15 1199 0029'

                                        UNION ALL

                                        SELECT
                                            "Reference"          AS reference,
                                            "Versement Numero"    AS num,
                                            trunc("Versement Date") AS date_versement,
                                            "Montant reglement par police" AS versement
                                            ,'ver2' as type
                                        FROM
                                            bi_versements
                                        WHERE
                                            trunc("Versement Date") <> TO_DATE('01/10/2020', 'dd/mm/yyyy')
                                            and trunc("Versement Date") <= trunc(current_date)
                                           -- and   "Reference" = '05619 15 1199 0029'



                                             UNION ALL  
                                        select reference ,
                                        0 as ech, 
                                        date_echeance,
                                        abs(montant_echeance)

                                        ,'prelo2' as type
                                        from bi_police_echeancier_reglement 
                                        where branche in ('1110','1540' ,'1543')
                                        and  montant_echeance < 0 
                                       -- and reference = '16013 18 1110 0117'


                                    ) v
                                WHERE
                                    r.reference = v.reference (+)
    -- and r.reference = '05619 15 1199 0029'
-- and r.num_echeance = 4
                                    AND trunc(date_souscription) <= trunc(current_date)
                            ) t
                    )

                    -----------FIN cumul -----------------
            ) r ,

            (
                SELECT
                    reference,
                    "Mode de Paiement" AS etat
                FROM
                    bi_data.bi_police_echeancier_reglement
                WHERE
                    "Mode de Paiement" IN (
                        'Perte',
                        'Contentieux'
                    )
                GROUP BY
                    reference,

                    "Mode de Paiement"
            ) con
        WHERE

      r.reference = con.reference (+)
        GROUP BY
            r.reference,
            r.num_echeance,
            trunc(r.date_souscription) ,
            r.mode_paiement,
            r.date_echeance,
            r.montant_echeance,
            r.date_reglement,

            con.etat

--having (r.montant_echeance- max(r.reglement_echeancier)) >10
    ) reg,
    bi_police p,

      (
                SELECT
                    substr(reference, 1, 18) reference,
                    num_aliment,
                    transaction, 
                    NUM_POLICE

                FROM
                    bi_police
                WHERE
                    transaction IN (
                        '19',
                        '30',
                        '49'
                    )

          --    and reference like '09004 12 3432 0008-9%'
                GROUP BY
                    substr(reference, 1, 18),
                    num_aliment, 
                    transaction,
                    NUM_POLICE

            ) i,
            (select reference ,num_echeance, count(num_echeance) as nombre from bi_police_echeancier_reglement
            group by reference ,num_echeance ) rr,
            
            (select reference , num_echeance, 'a' as mla_etat from  bi_police_echeancier_reglement
            where MONTANT_ECHEANCE <0 ) mla
            



WHERE
    p.reference = reg.reference
    and DATE_ECHEANCE >= to_date('01/01/2006','dd/mm/yyyy')
    and DATE_ECHEANCE < to_date('01/01/2100','dd/mm/yyyy')
    and substr(p.reference,1,18) = i.reference (+)
    and p.num_aliment = i.num_aliment(+)
    and p.NUM_POLICE = i. NUM_POLICE(+)
    and rr.reference = reg.reference 
    and rr.num_echeance =  reg.num_echeance 
    and   reg.num_echeance  =   mla.num_echeance(+) 
  and   reg.reference   =   mla.reference (+)

) t


"""
df_imp = pd.read_sql(imp, con=conn)
# print(df_imp.MONTANT.sum())
# df_imp = df_imp.loc[df_imp.MLA_ETAT != 'a']
# print(df_imp.MONTANT.sum())
imp4 = df_imp
# df_imp.to_csv(r'C:\Users\matemmani\impayes.csv')
# print(df_imp['MONTANT'])
imp5 = imp4

imp11 = df_imp



imp11.TRANSACTION.fillna('a', inplace=True)
imp11.ETAT_POLICE.fillna('a', inplace=True)

imp11 = imp11[['REFERENCE', 'SOUSCRIPTION', 'AGENCE', 'ETAT_POLICE', 'TRANSACTION', 'NUM_ECHEANCE', 'DATE_ECHEANCE',
               'MONTANT_ECHEANCE', 'DATE_REGLEMENT', 'REGLEMENT_ECHEANCIER']].reset_index()

imp5 = imp11.groupby( by=['REFERENCE', 'SOUSCRIPTION', 'AGENCE', 'ETAT_POLICE', 'TRANSACTION', 'NUM_ECHEANCE', 'DATE_ECHEANCE']).cumsum(
    skipna=False).fillna(0).reset_index()
imp11['cum'] = imp5['REGLEMENT_ECHEANCIER']

imp11['mincum'] = imp11['MONTANT_ECHEANCE'] - imp11['cum']
imp11['mincum'] = imp11['mincum'].astype(int)

imp11['new_ech'] = imp11['REGLEMENT_ECHEANCIER']

imp3 = imp11[['REFERENCE', 'SOUSCRIPTION', 'AGENCE', 'ETAT_POLICE', 'TRANSACTION', 'NUM_ECHEANCE', 'DATE_REGLEMENT',
              'mincum']]  # partie validé
imp4 = imp3.groupby(by=['REFERENCE', 'SOUSCRIPTION', 'AGENCE', 'ETAT_POLICE', 'TRANSACTION',
                        'NUM_ECHEANCE']).min().reset_index()  # partie validé
# df_imp['NUM_ECHEANCE'] =df_imp['NUM_ECHEANCE'].astype(str)
# df_imp['mincum'] =df_imp['mincum'].astype(int)

# imp11.to_csv(r'C:\Users\matemmani\imp11_2.csv',decimal=',')
imp12 = imp11.loc[imp11.new_ech > 0]
# imp13 = imp11.loc[imp11.new_ech > 0]

imp3 = imp11[['REFERENCE', 'SOUSCRIPTION', 'AGENCE', 'ETAT_POLICE', 'TRANSACTION', 'NUM_ECHEANCE', 'DATE_ECHEANCE',
              'DATE_REGLEMENT', 'mincum']]  # partie validé
imp4 = imp3.groupby(by=['REFERENCE', 'SOUSCRIPTION', 'AGENCE', 'ETAT_POLICE', 'TRANSACTION', 'NUM_ECHEANCE',
                        'DATE_ECHEANCE']).min().reset_index()  # partie validé
# imp4.to_csv(r'C:\Users\matemmani\imp4.csv',decimal=',')
imp4 = imp4.loc[imp4.mincum > 0]
# imp4.to_csv(r'C:\Users\matemmani\imp4_4.csv',decimal=',')
imp12['new_reg'] = imp12['new_ech']
imp12 = imp12[
    ['REFERENCE', 'SOUSCRIPTION', 'AGENCE', 'ETAT_POLICE', 'TRANSACTION', 'NUM_ECHEANCE', 'DATE_ECHEANCE', 'new_ech',
     'DATE_REGLEMENT', 'REGLEMENT_ECHEANCIER']]
imp4['REGLEMENT_ECHEANCIER'] = imp4['mincum'] - imp4['mincum']
imp4 = imp4[
    ['REFERENCE', 'SOUSCRIPTION', 'AGENCE', 'ETAT_POLICE', 'TRANSACTION', 'NUM_ECHEANCE', 'DATE_ECHEANCE', 'mincum',
     'DATE_REGLEMENT', 'REGLEMENT_ECHEANCIER']]
imp5 = pd.concat([imp12, imp4])
# imp5.to_csv(r'C:\Users\matemmani\imp5.csv',decimal=',')
imp5['ech'] = imp5['mincum'].fillna(0) + imp5['new_ech'].fillna(0)
imp6 = imp5[['REFERENCE', 'AGENCE', 'ETAT_POLICE', 'TRANSACTION', 'NUM_ECHEANCE', 'DATE_ECHEANCE', 'ech', 'DATE_ECHEANCE']]
# imp5.to_csv(r'C:\Users\matemmani\imp11_2.csv',decimal=',')


imp6 = imp5[ ['REFERENCE', 'SOUSCRIPTION', 'AGENCE', 'ETAT_POLICE', 'TRANSACTION', 'NUM_ECHEANCE', 'DATE_REGLEMENT', 'ech',
     'DATE_ECHEANCE']]
imp7 = imp5[['REFERENCE', 'SOUSCRIPTION', 'AGENCE', 'ETAT_POLICE', 'TRANSACTION', 'NUM_ECHEANCE', 'DATE_REGLEMENT',
             'REGLEMENT_ECHEANCIER', 'DATE_ECHEANCE']]

# imp7['DATE_REGLEMENT'] = imp7['DATE_REGLEMENT'].astype(str)
imp8 = pd.concat([imp6, imp7])
imp8['MONTANT'] = imp8['ech'].fillna(0) - imp8['REGLEMENT_ECHEANCIER'].fillna(0)
# imp8.to_csv(r'C:\Users\matemmani\imp8.csv',decimal=',')

#  [x+1 if x >= 45 else x+5 for x in l]

print(imp8['MONTANT'].sum())
# condition = [

#     (imp8['MONTANT'] >= 0),
#     (imp8['MONTANT'] < 0),
# ]


# values = [

#     imp8['DATE_ECHEANCE'],
#     imp8['DATE_REGLEMENT'],

# ]

# df_imp['mincum'] =df_imp['mincum'].astype(int)
imp8['datee'] = np.where(imp8['MONTANT'] >= 0, imp8['DATE_ECHEANCE'], imp8['DATE_REGLEMENT'])

imp8['date_sous'] = np.where(imp8['MONTANT'] >= 0, imp8['SOUSCRIPTION'], imp8['DATE_REGLEMENT'])

# imp8.to_csv(r'C:\Users\matemmani\imp11_3.csv',decimal=',')
# imp8.to_csv(r'C:\Users\matemmani\imp11_3.csv',decimal=',')
# imp8.to_csv(r'C:\Users\matemmani\imp11_3.csv', decimal=',')

imp9= imp8[['REFERENCE','SOUSCRIPTION','AGENCE','DATE_ECHEANCE','DATE_REGLEMENT','ETAT_POLICE','NUM_ECHEANCE','TRANSACTION','datee','MONTANT','date_sous']]
imp9 = imp9.sort_values('datee', ascending=True).reset_index()
df = imp9[['MONTANT']].cumsum(skipna=True)
imp9['cum2'] =df
imp9['TYPE'] = np.where((np.isnat(imp9.DATE_REGLEMENT)&  (imp9.DATE_ECHEANCE <  dt.datetime.today())),'echus',
                        np.where(imp9.DATE_REGLEMENT >imp9.DATE_ECHEANCE ,'echus','non echus'))
# imp9.to_csv(r'C:\Users\matemmani\impayes.csv',decimal=',')

imp4 = imp9
print(imp9['MONTANT'].sum())

imp4['datee'] = pd.to_datetime(imp4.datee)
imp4['SOUSCRIPTION'] = pd.to_datetime(imp4.SOUSCRIPTION)
df2 = imp4['datee'].dt.year


imp4['YEAR_ALL'] = df2

df4 = imp4['SOUSCRIPTION'].dt.year
imp4['YEAR_SOUS'] = df4
imp6= imp4
# imp4 = imp4 = imp4.loc[imp4.MONTANT != 0]
engine = create_engine('postgresql://postgres:Amaterasou123@localhost:5432/DCG')
con_post = engine.connect()
df_r = """select * from public.reseau"""
df_reseau = pd.read_sql(df_r, con_post)
print(imp4['date_sous'])
print(imp4[['date_sous']])
imp4['date_sous'] =pd.to_datetime(imp4['date_sous'],  format='%d/%m/%y').dt.date
imp4['datee'] =pd.to_datetime(imp4['datee'],  format='%d/%m/%y').dt.date
# imp4 = imp4.loc[imp4.MONTANT !=0]
imp4['agencenum'] = imp4['AGENCE'].astype(int)
imp4= imp4.merge(df_reseau[['Agence', 'STATUT', 'CODE AGENCE', 'direction', 'nom','Agence 3', ]],
                                        left_on='agencenum',
                                        right_on='Agence')
imp4 = imp4.loc[imp4.TRANSACTION == 'a']
imp4 = imp4.loc[imp4.MONTANT != 0]
imp4['BRANCHE'] =  imp4['REFERENCE'].str[9:13]
# imp4 = imp4[['REFERENCE','SOUSCRIPTION','AGENCE','ETAT_POLICE','datee',  'MONTANT','date_sous','TYPE','direction', 'CODE AGENCE','BRANCHE']]
imp4 = imp4.drop(['Agence'], axis=1)

imp4['TYPE BRANCHE'] = np.where( ( (imp4['BRANCHE'] == '1110')  | (imp4['BRANCHE'] == '1194')| (imp4['BRANCHE'] == '1540')
                                  |(imp4['BRANCHE'] == '1543')) , 'MLA', 'HORS MLA')


imp4.to_csv(r'C:\Users\matemmani\PycharmProjects\import imp\impayes.csv', decimal=',')

DRC_OUEST = imp4.loc[imp4.direction == 'DRC OUEST']
DRC_OUEST.to_csv(r'C:\Users\matemmani\PycharmProjects\import imp\DRC OUEST.csv', decimal=',')

DRC_EST = imp4.loc[imp4.direction == 'DRC EST']
DRC_EST.to_csv(r'C:\Users\matemmani\PycharmProjects\import imp\DRC EST.csv', decimal=',')

DR_BLIDA = imp4.loc[imp4.direction == 'DR BLIDA']
DR_BLIDA.to_csv(r'C:\Users\matemmani\PycharmProjects\import imp\DR BLIDA.csv', decimal=',')

DRA = imp4.loc[imp4.direction == 'DRA']
DRA.to_csv(r'C:\Users\matemmani\PycharmProjects\import imp\DRA.csv', decimal=',')

DRO = imp4.loc[imp4.direction == 'DRO']
DRO.to_csv(r'C:\Users\matemmani\PycharmProjects\import imp\DRO.csv', decimal=',')

DRS = imp4.loc[imp4.direction == 'DRS']
DRS.to_csv(r'C:\Users\matemmani\PycharmProjects\import imp\DRS.csv', decimal=',')

DRT = imp4.loc[imp4.direction == 'DRT']
DRT.to_csv(r'C:\Users\matemmani\PycharmProjects\import imp\DRT.csv', decimal=',')

print(imp4['MONTANT'].sum())