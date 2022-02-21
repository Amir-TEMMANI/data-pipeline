import shutil
from datetime import date
import pandas as pd
today = date.today()
d1 = today.strftime("%d-%m-%Y")

def actu():
    original = r"\\qnap\dcg\Réseau actualisé.xlsx"
    target = r'\\qnap\dcg\DCG_data warehouse\reseau\Réseau actualisé'+d1+'.xlsx'

    shutil.copyfile(original, target)
    print('')
    reseau = pd.read_excel(r"\\qnap\dcg\Réseau actualisé.xlsx", 'Direction')

    reseau = reseau[['Agence', 'CODE AGENCE', 'direction', 'STATUT', 'OBSERVATION', 'nom', 'Agence 3']]
    print(reseau)
    reseau.to_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\reseau.gzip',
              compression='gzip')
    target = r'\\qnap\dcg\DCG_data warehouse\config\reseau.gzip'
    reseau.to_parquet(r'\\qnap\dcg\DCG_data warehouse\config\reseau.gzip',
              compression='gzip')



    branche = pd.read_excel(r"\\qnap\dcg\Réseau actualisé.xlsx", 'Branche')
    branche = branche[['Code branche', 'Branche1', 'Branche 2']]
    branche.to_parquet(r'C:\Users\matemmani\PycharmProjects\import reseau\branche.gzip',
                      compression='gzip')
    branche.to_parquet(r'\\qnap\dcg\DCG_data warehouse\config\branche.gzip',
                      compression='gzip')
                
if __name__ == '__main__':
    actu()
