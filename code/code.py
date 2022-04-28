from textwrap import indent
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import os
import pandas as pd
import numpy as np
import shutil
from pandasql import sqldf
import json

DATA_PATH = '/home/mehdi/airflow/data/dw-project2'
INPUT = DATA_PATH + '/input/data.json'
TMP = DATA_PATH + '/tmp'
OUTPUT = DATA_PATH + '/output'



def _load_file():
    with open(INPUT) as f:
        data = json.load(f)

        places = data['places']

        #cleaning, deleting useless data
        id = 1
        for place in places:
            del place['url']
            del place['name']
            del place['description']
            del place['stars']
            del place['reviews']
            del place['breakfast']
            del place['checkInFrom']
            del place['checkInTo']
            del place['location']
            del place['image']
            del place['images']
            del place['order']
            place['id'] = id
            id = id + 1
        
        with open(TMP + '/1.json', 'w+') as e:
           json.dump(places,e)



def _get_distinct_values():

    with open(TMP + '/1.json') as f:
        places = json.load(f)
        distinct_place_types = list(set([i['type'] for i in places]))
        distinct_postal_codes = list(set([i['address']['postalCode'] for i in places])) 

        distinct_addresses = []
        for i in  distinct_postal_codes:
            address = dict()
            for j in places:
                if j['address']['postalCode'] == i:
                    address['postal_code'] = j['address']['postalCode']
                    address['street'] = j['address']['street'] 
                    address['region'] = j['address']['region']
                    address['place_id'] = j['id']
                    distinct_addresses.append(address)
                    break


        all_rooms = []
        for i in places:
            for j in i['rooms']:
                j['place_id'] = i['id']
                del j['url']
                del j['bedType']
                all_rooms.append(j)

        distinct_room_types = list(set([i['roomType'] for i in all_rooms]))
        distinct_number_of_persons = list(set([i['persons'] for i in all_rooms]))        

        with open(TMP + '/distinct_place_types.json', 'w+') as e:
            json.dump(distinct_place_types,e)
        
        with open(TMP + '/distinct_addresses.json', 'w+') as e:
            json.dump(distinct_addresses,e)
        
        with open(TMP + '/distinct_postal_codes.json', 'w+') as e:
            json.dump(distinct_postal_codes,e)

        with open(TMP + '/distinct_room_types.json', 'w+') as e:
            json.dump(distinct_room_types,e)

        with open(TMP + '/distinct_number_of_persons.json', 'w+') as e:
            json.dump(distinct_number_of_persons,e)

def _group_by():
    with open(TMP + '/1.json') as f:
        places = json.load(f)
        before_groupby = []
        for place in places:
            for room in place['rooms']:
                line = dict()
                line['place_type'] = place['type']
                line['postal_code'] = place['address']['postalCode'] 
                line['room_type'] = room['roomType']
                line['number_of_persons'] = room['persons']
                line['rating'] = place['rating']
                before_groupby.append(line)
                
        df = pd.DataFrame(before_groupby)
        query = '''SELECT number_of_persons, postal_code, room_type, place_type,  
        COUNT(*) AS number_of_offers,
        AVG(rating) AS average_rating, 
        MAX(rating) AS max_rating, 
        MIN(rating) AS min_rating
        from df
        GROUP BY place_type, postal_code, room_type, number_of_persons
        ORDER BY number_of_offers DESC
        '''
        tmp_df = sqldf(query,locals())   

        tmp_dict = json.loads(tmp_df.to_json(orient="records"))
        with open(TMP + '/tmp_view.json', 'w+') as e:
            json.dump(tmp_dict,e)

def _join():

    distinct_place_types = []
    distinct_room_types = []
    distinct_number_of_persons = []

    with open(TMP + '/distinct_place_types.json') as f:
        distinct_place_types = json.load(f)

    with open(TMP + '/distinct_addresses.json') as f:
        distinct_addresses = json.load(f)

    with open(TMP + '/distinct_room_types.json') as f:
        distinct_room_types = json.load(f)

    with open(TMP + '/distinct_number_of_persons.json') as f:
        distinct_number_of_persons = json.load(f)

    tmp = dict()
    with open(TMP + '/tmp_view.json') as f:
        tmp = json.load(f)
    

    ### tmp df

    tmp_df = pd.DataFrame.from_dict(tmp)

    ##### TABLES DIMENSIONS #######
    
    d_addresses_df = pd.DataFrame(distinct_addresses)
    d_addresses_df.insert(loc=0, column='id', value=np.arange(len(d_addresses_df)))
    d_addresses_df.drop('place_id', inplace=True, axis=1)

    d_place_types_df = pd.DataFrame(distinct_place_types)
    d_place_types_df.insert(loc=0, column='id', value=np.arange(len(d_place_types_df)))
    d_place_types_df.columns = ['id', 'place_type']

    d_room_types_df = pd.DataFrame(distinct_room_types)
    d_room_types_df.insert(loc=0, column='id', value=np.arange(len(d_room_types_df)))
    d_room_types_df.columns = ['id', 'room_type']

    d__number_of_persons_df = pd.DataFrame(distinct_number_of_persons)
    d__number_of_persons_df.insert(loc=0, column='id', value=np.arange(len(d__number_of_persons_df)))
    d__number_of_persons_df.columns = ['id', 'number']
    
    #display(d_addresses_df)

    ###### TABLE FAIT ####
    query = '''SELECT 
    a.id AS address_id, 
    p.id AS place_type_id,
    r.id AS room_type_id, 
    n.id AS number_of_persons_id,
    t.number_of_offers, t.average_rating, t.max_rating, t.min_rating  
    FROM tmp_df t
    INNER JOIN d_addresses_df a on t.postal_code = a.postal_code
    INNER JOIN d_place_types_df p on t.place_type = p.place_type
    INNER JOIN d_room_types_df r on t.room_type = r.room_type
    INNER JOIN d__number_of_persons_df n on t.number_of_persons = n.number
    '''
    fact_table_df = sqldf(query,locals())
    fact_table_df.to_csv(OUTPUT + '/offers.csv') 
    tmp_dict = json.loads(fact_table_df.to_json(orient="records"))
    with open(OUTPUT + '/offers.json', 'w+') as e:
        json.dump(tmp_dict,e)


def _delete_tmp_files():
    shutil.rmtree(TMP)    
    

with DAG("my_second_dag", start_date=datetime(2022, 4, 3), schedule_interval="@once", catchup=False ) as dag:
    
    load_file = PythonOperator(
        task_id = "load_file",
        python_callable = _load_file
    )


    get_distinct_values = PythonOperator(
        task_id = "get_distinct_values",
        python_callable = _get_distinct_values
    )

    group_by = PythonOperator(
        task_id = "group_by",
        python_callable = _group_by
    )

    join = PythonOperator(
        task_id = "join",
        python_callable = _join
    )


    delete_tmp_files = PythonOperator(
        task_id = "delete_tmp_files",
        python_callable = _delete_tmp_files
    )

    load_file >> get_distinct_values >> join >> delete_tmp_files

    load_file >> group_by >> join >> delete_tmp_files

