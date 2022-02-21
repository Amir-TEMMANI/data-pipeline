from sqlalchemy import create_engine
import pandas as pd
import cx_Oracle

# from os import listdir
# from os.path import isfile, join
# mypath = r'C:\'
# onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

####
engine = create_engine('postgresql://postgres:Amaterasou123@localhost:5432/DCG')


df_reseau = pd.read_excel(r"\\qnap\dcg\Réseau actualisé.xlsx", 'Direction')
df_reseau= df_reseau[['Agence', 'CODE AGENCE', 'direction','STATUT','OBSERVATION', 'nom','Agence 3' ]]
print('4')
df_Branche = pd.read_excel(r"\\qnap\dcg\Réseau actualisé.xlsx", 'Branche')
df_Branche = df_Branche[['Code branche', 'Branche1', 'Branche 2']]
print('5')




df_reseau.to_sql(name = 'reseau', con= engine , index = False , if_exists ='replace',method='multi')
print('6')
df_Branche.to_sql(name = 'Branche', con= engine , index = False , if_exists ='replace',method='multi')
print('7')
#### ici
# df_mois = pd.read_excel(r"C:\Users\matemmani\Desktop\Mois.xlsx", 'Feuil1')
# df_mois.to_sql(name = 'Mois', con= engine , index = False , if_exists ='replace')


# production_ann = """
#         SELECT
#         prod.agence,
#         prod.branche,
#         prod.souscription,            nette AS nette
#
#         FROM
#
#                     bi_police prod INNER JOIN bi_police_prime   prime ON prime.reference = prod.reference
#                 WHERE
#                     valider = 1
#                     AND prod.document <> '6'
#                     and code NOT IN (
#                         '2',
#                         '3',
#                         '4',
#                         '5',
#                         '6'
#                     )
#                     AND ( ( trunc(souscription, 'y') = trunc(current_date, 'y')
#                             AND souscription <= current_date )
#                           OR ( trunc(souscription, 'y') = trunc(add_months(current_date, - 12), 'y')
#                                AND souscription <= add_months(current_date, - 12) ) )"""
#
# production_ann = pd.read_sql(production_ann, con=conn)
# print(production_ann)
# production_ann.to_sql(name = 'prod_annee', con= engine , index = False , if_exists ='replace')
#
# print(production_ann)