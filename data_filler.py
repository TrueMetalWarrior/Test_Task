import datetime
import json
import uuid
import random
import psycopg2
from DB_config import load_config

inns = ['owner_1', 'owner_2', 'owner_3', 'owner_4']
status = [1, 2, 3, 4, 10, 13]
d_type = ['transfer_document', 'not_transfer_document']


def make_data() -> dict:
    """Генерация рандомных данных для таблицы data в базе, вернёт list, внутри dict по каждой записи"""
    parents = set()
    children = dict()
    data_table = dict()

    for _ in list(range(0, 20)):
        parents.add('p_' + str(uuid.uuid4()))

    for p in parents:
        children[p] = set()
        for _ in list(range(0, 50)):
            children[p].add('ch_' + str(uuid.uuid4()))

    for k, ch in children.items():
        data_table[k] = {'object': k,
                         'status': random.choice(status),
                         'owner': random.choice(inns),
                         'level': 1,
                         'parent': None}

        for x in ch:
            data_table[x] = {'object': x,
                             'status': random.choice(status),
                             'owner': data_table[k]['owner'],
                             'level': 0,
                             'parent': k}
    return data_table


def make_documents(data: dict) -> list:
    """Генерация рандомных данных для таблицы documents в базе, вернёт list, внутри dict по каждой записи"""
    result = list()
    doc_count = random.choice(list(range(10, 20)))
    for _ in range(doc_count):
        result.append(__make_doc(data))
    return result


def __make_doc(data: dict) -> dict:
    saler = reciver = random.choice(inns)
    while saler == reciver:
        reciver = random.choice(inns)

    doc = dict()
    dd = doc['document_data'] = dict()
    dd['document_id'] = id = str(uuid.uuid4())
    dd['document_type'] = random.choice(d_type)

    doc['objects'] = [x for x, v in data.items() if v['level'] == 1 and v['owner'] == saler]

    md = doc['operation_details'] = dict()

    if random.choice([0, 1]):
        mds = md['status'] = dict()
        mds['new'] = mds['old'] = random.choice(status)
        while mds['old'] == mds['new']:
            mds['new'] = random.choice(status)

    if dd['document_type'] == d_type[0]:
        mdo = md['owner'] = dict()
        mdo['new'] = mdo['old'] = random.choice(inns)
        while mdo['old'] == mdo['new']:
            mdo['new'] = random.choice(inns)

    doc_data = {'doc_id': id,
                'recieved_at': datetime.datetime.now(),
                'document_type': dd['document_type'],
                'document_data': json.dumps(doc)}
    return doc_data



def DB_Fill(documents_tbl, data_tbl):
    try:
        config = load_config()
        connectiondb = psycopg2.connect(**config)
        print('Connection established!')
        with connectiondb.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.data (
                object character varying(50) COLLATE pg_catalog."default" NOT NULL,
                status integer,
                level integer,
                parent character varying COLLATE pg_catalog."default",
                owner character varying(14) COLLATE pg_catalog."default",
                CONSTRAINT data_pkey PRIMARY KEY (object)
            )
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.documents (
                doc_id character varying COLLATE pg_catalog."default" NOT NULL,
                recieved_at timestamp without time zone,
                document_type character varying COLLATE pg_catalog."default",
                document_data jsonb,
                processed_at timestamp without time zone,
                CONSTRAINT documents_pkey PRIMARY KEY (doc_id)
            )
            """)
            for i in data_tbl:
                cursor.execute('INSERT INTO data (object, status, level, parent, owner) VALUES (%s, %s, %s, %s, %s)',
                             (i['object'], i['status'], i['level'], i['parent'], i['owner']))
            connectiondb.commit()
            for i in documents_tbl:
                cursor.execute('INSERT INTO documents (doc_id, recieved_at, document_type, document_data) VALUES (%s, %s, %s, %s)',
                             (i['doc_id'], i['recieved_at'], i['document_type'], i['document_data']))
            connectiondb.commit()
            cursor.close()
        connectiondb.close()
    except Exception as Error:
        print(Error)


if __name__ == '__main__':
    data = make_data()
    # данные для базы:
    data_tbl = list(data.values())
    documents_tbl = make_documents(data)
    DB_Fill(documents_tbl, data_tbl)

