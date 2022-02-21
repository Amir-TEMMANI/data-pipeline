import datetime as dt
import  pandas as pd



mois = pd.read_parquet(r'\\qnap\dcg\DCG_data warehouse\config\mois.gzip')


todaym = dt.datetime.today()
moisM  = todaym.month

mois = mois.loc[mois.Mois_num <= moisM]


mois.to_excel(r'\\qnap\dcg\DCG_data warehouse\config\mois.xlsx')