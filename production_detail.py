import cx_Oracle
import datetime as dt
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import os

dsn_tns = cx_Oracle.makedsn('****', '****',
                            service_name='****')  # if needed, place an 'r' before any parameter in order to
# address special characters such as '\'.
conn = cx_Oracle.connect(user=r'****', password='****',
                         dsn=dsn_tns)

prod = """select p.reference,  p.agence , p.branche , p.annee_police , p.num_police  ,p.classe , p.souscription  , p.expiration , p.effet, pp.code , p.transaction, pp.risque, sum(pp.nette) as nette ,sum(dasc.nette) as dasc  , sum(dc.nette) as dc  from bi_police p, 
bi_police_prime pp , (select  reference as reference, agence , branche , annee_police , num_police  , risque , 1 as nette from bi_police_prime where code in ('030141',
'030142',
'030143',
'030144',
'030145',
'030145',
'030146',
'030146',
'030147',

'030148',
'030149')and document <> 6 )dasc , 

 (select  reference as reference, agence , branche , annee_police , num_police  , risque , 1 as nette from bi_police_prime where code in ('030110',
'030111',
'030112',
'030113',
'030114',
'030115',
'030116',
'030117'
) and document <> 6 )dc 

where 
p.reference = pp.reference
and pp.agence = dasc.agence (+)
and pp.branche = dasc.branche (+)
and pp.annee_police = dasc.annee_police (+)
and pp.num_police = dasc.num_police (+)
and pp.risque = dasc.risque (+)


and pp.agence = dc.agence (+)
and pp.branche = dc.branche (+)
and pp.annee_police = dc.annee_police (+)
and pp.num_police = dc.num_police (+)

and pp.risque = dc.risque (+)
and p.valider = 1
and p.document <> 6
and code not in (2,3,4,5,6)
 and trunc(souscription,'y') >= trunc(add_months(current_date, - 12),'y')
 and trunc(souscription) <= trunc(current_date)

group by p.reference,  p.agence , p.branche , p.annee_police , p.num_police  ,p.classe , p.souscription  , p.expiration , p.effet, pp.code, p.agence , p.branche , p.transaction, pp.risque

"""

prod = pd.read_sql(prod, con=conn)
reseau = pd.read_parquet(r'\\qnap\dcg\DCG_data warehouse\config\reseau.gzip')
branche = pd.read_parquet(r'\\qnap\dcg\DCG_data warehouse\config\branche.gzip').drop_duplicates()
codi= pd.read_csv(r'\\qnap\dcg\DCG_data warehouse\codification\codification.csv')
trans= pd.read_excel(r'\\qnap\dcg\DCG_data warehouse\config\transaction.xlsx')
print(trans.dtypes)
print(prod.dtypes)
prod['CODE'] = prod['CODE'].astype(int)
prod['TRANSACTION'] = prod['TRANSACTION'].astype(int)
prod['AGENCE'] = prod['AGENCE'].astype(int)
prod['BRANCHE'] = prod['BRANCHE'].astype(int)

prod['EXPIRATION'] = prod['EXPIRATION'].fillna('2000-01-07')
prod['EFFET'] = prod['EFFET'].fillna('2000-01-07')

prod = prod.merge(branche[['Code branche', 'Branche1', 'Branche 2', ]],  left_on='BRANCHE', right_on='Code branche')

prod = prod.merge(codi[['CODE','Nom rubrique', ]],      left_on='CODE',     right_on='CODE')

prod = prod.merge(trans[['Fils','nombre', ]],      left_on='TRANSACTION',     right_on='Fils')

prod = prod.merge(reseau[['Agence','CODE AGENCE', 'direction', ]],
                        left_on='AGENCE',
                        right_on='Agence')

condition_annee = [
    (prod['SOUSCRIPTION'].dt.year == dt.datetime.today().year),
    (prod['SOUSCRIPTION'].dt.year == dt.datetime.today().year - 1),

]

valeur_annee = [
    (1),
    (0),
]

prod['TYPE_ANNE'] = np.select(condition_annee, valeur_annee)

today = dt.datetime.today().strftime("%Y-%m-%d")
today = dt.datetime.strptime(today, "%Y-%m-%d")
prod['EXPIRATION2'] = pd.to_datetime(prod['EXPIRATION']).astype(np.int64)
prod['EFFET2'] = pd.to_datetime(prod['EFFET']).astype(np.int64)
prod['SOUSCRIPTION2'] = pd.to_datetime(prod['SOUSCRIPTION']).astype(np.int64)

prod['today'] = today
prod['today'] = pd.to_datetime(prod['today']).astype(np.int64)

# today.astype(np.int64)


# prod['duree'] = prod['EXPIRATION'] -today
# # prod['duree'].astypes(int)
# prod['duree']

zero_jour = 0

prod['duree'] = prod['EXPIRATION2'] - prod['today']

condition_duree = [prod['TYPE_ANNE'] == 1,
                   prod['TYPE_ANNE'] == 0
                   ]

valeur_duree = [
    prod['duree'],
    0

]
prod['duree'] = np.select(condition_duree, valeur_duree)

prod['rec'] = np.where((prod['TYPE_ANNE'] == 1) & (prod['EXPIRATION2'] > prod['today']) & (prod['CODE'] != 1),
                       prod['NETTE'] * 0.31, 0)


prod['prorata'] = np.where( (prod['TYPE_ANNE'] == 1)
                           & ( prod['EXPIRATION2'] > prod['today'] )
                           & ( prod['CODE'] != 1 ) , prod['NETTE']/ (prod['EXPIRATION2'] -prod['EFFET2'] +1)*prod['duree'] ,0)

prod['PPNA_N'] = np.where( (prod['TYPE_ANNE'] == 1) & (( prod['BRANCHE'] == 1322) |  ( prod['BRANCHE'] == 1321))
                           & ( prod['CODE'] != 1 ) , prod['prorata'] , (
np.where((prod['TYPE_ANNE'] == 1) & ( prod['BRANCHE'] == 1331 )  & ( prod['CODE'] != 1 ),0 , prod['rec'])))


datey1 = dt.datetime(today.year-1, 12, 31).strftime("%Y-%m-%d")
prod['datey1'] = datey1
prod['datey1'] = pd.to_datetime(prod['datey1']).astype(np.int64)

prod['duree2'] = prod['EXPIRATION2'] - prod['datey1']

condition_duree = [prod['TYPE_ANNE'] == 0,
                   prod['TYPE_ANNE'] == 1
                   ]

valeur_duree = [
    prod['duree2'],
    0

]
prod['duree2'] = np.select(condition_duree, valeur_duree)


prod['rec_31_12'] =np.where( (prod['TYPE_ANNE'] == 0)
                             & ( prod['EXPIRATION2'] > prod['datey1'] )
                             & ( prod['CODE'] != 1 ) , prod['NETTE'] * 0.32,0)

prod['prorata2'] = np.where( (prod['TYPE_ANNE'] == 0)
                           & ( prod['EXPIRATION2'] > prod['datey1'] )
                           & ( prod['CODE'] != 1 ) , prod['NETTE']/ (prod['EXPIRATION2'] -prod['EFFET2'] +1)*prod['duree2'] ,0)

prod['PPNA_31_21'] = np.where( (prod['TYPE_ANNE'] == 0) & (( prod['BRANCHE'] == 1322) |  ( prod['BRANCHE'] == 1321))
                               & ( prod['CODE'] != 1 ) , prod['prorata2'] , (
np.where((prod['TYPE_ANNE'] == 0) & ( prod['BRANCHE'] == 1331 )  & ( prod['CODE'] != 1 ),0 , prod['rec_31_12'])))


today_n1 = (dt.datetime.today() - relativedelta(years=1)).strftime("%Y-%m-%d")
prod['today_n1'] = today_n1
prod['today_n1'] = pd.to_datetime(prod['today_n1']).astype(np.int64)

prod['duree3'] = prod['EXPIRATION2'] - prod['today_n1']

condition_duree = [prod['TYPE_ANNE'] == 0,
                   prod['TYPE_ANNE'] == 1
                   ]

valeur_duree = [
    prod['duree3'],
    0

]
prod['duree3'] = np.select(condition_duree, valeur_duree)

prod['rec_n_1'] = np.where((prod['TYPE_ANNE'] == 0) & (prod['EXPIRATION2'] > prod['today']) &
                           (prod['CODE'] != 1) & (prod['SOUSCRIPTION2'] > prod['today_n1']), prod['NETTE'] * 0.32, 0)


prod['prorata3'] = np.where( (prod['TYPE_ANNE'] == 0)
                           & ( prod['EXPIRATION2'] > prod['today'] )
                            & ( prod['SOUSCRIPTION2'] > prod['today_n1'] )
                           & ( prod['CODE'] != 1 ) , prod['NETTE']/ (prod['EXPIRATION2'] -prod['EFFET2'] +1)*prod['duree3'] ,0)


prod['PPNA_N_1_N'] = np.where( (prod['TYPE_ANNE'] == 0) & (( prod['BRANCHE'] == 1322) |  ( prod['BRANCHE'] == 1321))
                               & ( prod['CODE'] != 1 ) , prod['prorata3'] , (
np.where((prod['TYPE_ANNE'] == 0) & ( prod['BRANCHE'] == 1331 )  & ( prod['CODE'] != 1 ),0 , prod['rec_n_1'])))

prod =prod[['REFERENCE','CLASSE','SOUSCRIPTION','EXPIRATION','EFFET','CODE','Agence','BRANCHE','TRANSACTION','RISQUE','NETTE','DASC','DC','Branche1','Branche 2','Nom rubrique',
       'nombre','CODE AGENCE','direction','TYPE_ANNE','PPNA_N','PPNA_31_21','PPNA_N_1_N']]


prod.to_csv(r'\\qnap\dcg\DCG_data warehouse\production\production_global.csv')