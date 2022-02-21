
import cx_Oracle
import datetime as dt
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

def production():

                                   
      ##########ARIISSSS""""""""""""""""


    dsn_tns = cx_Oracle.makedsn('****', '****',
                                    service_name='****')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'****', password='****',
                                 dsn=dsn_tns)

    productiondd = """SELECT
            prod.agence,
            prod.branche,
            trunc(prod.souscription) as SOUSCRIPTION,
                sum(nette) AS nette
    
            FROM
    
                    bi_police prod INNER JOIN bi_police_prime   prime ON prime.reference = prod.reference
                WHERE
                    valider = 1
                    AND prod.document <> '6'
                    and code NOT IN (
                        '2',
                        '3',
                        '4',
                        '5',
                        '6'
                    )
                    AND  ( trunc(souscription) = trunc(current_date)
    
    
                          OR  trunc(souscription) = trunc(add_months(current_date, - 12)))
             
                    
                
                           group by  prod.agence,
            prod.branche,
            prod.souscription
    
    
                             union all
                           select distinct agence , branche , current_date , 0 from bi_police  
                           union all
                           select distinct agence , branche , trunc(add_months(current_date, - 12)) , 0 from bi_police
             """

    productionmm = """
                SELECT
        prod.agence,
        prod.branche,
        trunc(prod.souscription) as SOUSCRIPTION,
           sum(nette) AS nette
    
    FROM
    
                bi_police prod INNER JOIN bi_police_prime   prime ON prime.reference = prod.reference
            WHERE
                valider = 1
                AND prod.document <> '6'
                and code NOT IN (
                    '2',
                    '3',
                    '4',
                    '5',
                    '6'
                )
                AND ( (trunc(souscription, 'mm') = trunc(current_date, 'mm')
                        AND trunc(souscription) <= trunc(current_date) )
                      OR
                      ( trunc(souscription, 'mm') = trunc(add_months(current_date, - 12), 'mm')
                          AND souscription <= add_months(current_date, - 12) )) 
        
                
                           group by  prod.agence,
            prod.branche,
            prod.souscription
                          union all
                           select distinct agence , branche , current_date , 0 from bi_police  
                           union all
                           select distinct agence , branche , trunc(add_months(current_date, - 12)) , 0 from bi_police
            """

    productionyy = """SELECT
            prod.agence,
            prod.branche,
            trunc(prod.souscription) as SOUSCRIPTION,            sum(nette) AS nette
    
            FROM
    
                        bi_police prod INNER JOIN bi_police_prime   prime ON prime.reference = prod.reference
                    WHERE
                        valider = 1
                        AND prod.document <> '6'
                        and code NOT IN (
                            '2',
                            '3',
                            '4',
                            '5',
                            '6'
                        )
                        AND ( ( trunc(souscription, 'y') = trunc(current_date, 'y')
                                AND souscription <= current_date )
                              OR ( trunc(souscription, 'y') = trunc(add_months(current_date, - 12), 'y')
                                   AND souscription <= add_months(current_date, - 12) ) )

                          
                        
                        
                        
                                   group by  prod.agence,
            prod.branche,
            prod.souscription
                                    union all
                           select distinct agence , branche , current_date , 0 from bi_police  
                           union all
                           select distinct agence , branche , trunc(add_months(current_date, - 12)) , 0 from bi_police
    
                                   """


    df_productiondd = pd.read_sql(productiondd, con=conn)


    df_productionmm = pd.read_sql(productionmm, con=conn)


    df_productionyy = pd.read_sql(productionyy, con=conn)



    print(df_productiondd)

    print('DD')
    
    df_productiondd.to_parquet(r'C:\Users\matemmani\PycharmProjects\import production\df_productiondd.gzip',
                          compression='gzip')
    

    
    df_productionmm.to_parquet(r'C:\Users\matemmani\PycharmProjects\import production\df_productionmm.gzip',
                          compression='gzip')
    
    
    df_productionyy.to_parquet(r'C:\Users\matemmani\PycharmProjects\import production\df_productionyy.gzip',
                          compression='gzip')
    
    
    
if __name__== "__main__":
    production()
    

