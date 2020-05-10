# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 12:01:20 2020

@author: imano

pip install the following:
google-cloud-bigquery (para interactuar con bigquery)
google-api-python-client (para usar la API para Python)
"""

import imaplib
import email
import re
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# userName = input('Enter your GMail username:')
# passwd = getpass.getpass('Enter your password: ')
userName = 'bedrockcompetencia@gmail.com'
passwd = 'dm6aM*K]S?'

imapSession = imaplib.IMAP4_SSL('imap.gmail.com')
typ, accountDetails = imapSession.login(userName, passwd)

imapSession.select(mailbox='INBOX', readonly=False)
typ, emails = imapSession.search(None, '(FROM "bedrockcompetencia@gmail.com" SUBJECT "kantar edreams" UNSEEN)')
# https://gist.github.com/martinrusev/6121028

# num = b'4'
for num in emails[0].split():
    imapSession.store(num, '+FLAGS', '\Seen')
    typ, messageParts = imapSession.fetch(num, '(RFC822)')
    m = email.message_from_bytes(messageParts[0][1])

    for part in m.walk():
        data = part.get_payload(decode=True)

    data = "".join(chr(x) for x in data)

    # find what we want
    start = re.search("Campaña", data).start()
    end = re.search("TABULACIÓN", data).start()

    # select what we want
    data = data[start:(end - 5)]

    # take the colnames
    endheader = re.search("\n", data).end()
    header = data[0:endheader - 1]

    # take the data
    datos_de_verdad = data[endheader:end]
    endtotal = re.search("\r\n", datos_de_verdad).end()
    last = datos_de_verdad.rfind("\r\n")
    datos_de_verdad = datos_de_verdad[endtotal:last]

    # create a dataframe with the data
    df = pd.DataFrame([x.split('\t') for x in datos_de_verdad.split('\r\n')])
    # add headers to the dataframe
    df.columns = header.split("\t")
    # and then change colnames
    df.columns = ['Campaign', 'Marca', 'Modelo', 'Sector', 'Grupo', 'Producto',
                  'Anunciante', 'Cadena', 'Ambitos_de_emision', 'Fecha',
                  'Dia_semana', 'Hora_inicio', 'Duracion',
                  'Tipo', 'Formato', 'Titulo_emision', 'Literal_pos_bloque_2',
                  'Num_semana', 'Mes', 'Franja', 'Descripcion_creatividad',
                  'GRP', 'GRP_20seg', 'Inserciones']

    # some changes of types in the dataframe are now needed
    # change , to . in the numbers: 1,2 -> 1.2
    df = df.apply(lambda x: x.str.replace(',', '.'))

    # and then format them as numbers
    df.GRP = df.GRP.astype(float)
    df.GRP_20seg = df.GRP_20seg.astype(float)
    df.Inserciones = df.Inserciones.astype(int)

    # convert Fecha to datetime
    df.Fecha = pd.to_datetime(df.Fecha, format='%d/%m/%Y')
    # the dataframe is finally ready to be uploaded to bigquery

    # load the credentials, project, etc
    os.chdir("C:/Users/imano/Desktop/Competencia")

    credentials = service_account.Credentials.from_service_account_file(
        'credentials.json')

    project_id = 'dashboards-competencia'

    client = bigquery.Client(credentials=credentials, project=project_id)

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            bigquery.SchemaField(name="Campaign", field_type="STRING"),
            bigquery.SchemaField(name="Marca", field_type="STRING"),
            bigquery.SchemaField(name="Modelo", field_type="STRING"),
            bigquery.SchemaField(name="Sector", field_type="STRING"),
            bigquery.SchemaField(name="Grupo", field_type="STRING"),
            bigquery.SchemaField(name="Producto", field_type="STRING"),
            bigquery.SchemaField(name="Anunciante", field_type="STRING"),
            bigquery.SchemaField(name="Cadena", field_type="STRING"),
            bigquery.SchemaField(name="Ambitos_de_emision", field_type="STRING"),
            bigquery.SchemaField(name="Fecha", field_type="TIMESTAMP"),
            bigquery.SchemaField(name="Dia_semana", field_type="STRING"),
            bigquery.SchemaField(name="Hora_inicio", field_type="STRING"),
            bigquery.SchemaField(name="Duracion", field_type="STRING"),
            bigquery.SchemaField(name="Tipo", field_type="STRING"),
            bigquery.SchemaField(name="Formato", field_type="STRING"),
            bigquery.SchemaField(name="Titulo_emision", field_type="STRING"),
            bigquery.SchemaField(name="Literal_pos_bloque_2", field_type="STRING"),
            bigquery.SchemaField(name="Num_semana", field_type="STRING"),
            bigquery.SchemaField(name="Mes", field_type="STRING"),
            bigquery.SchemaField(name="Franja", field_type="STRING"),
            bigquery.SchemaField(name="Descripcion_creatividad", field_type="STRING"),
            bigquery.SchemaField(name="GRP", field_type="FLOAT"),
            bigquery.SchemaField(name="GRP_20seg", field_type="FLOAT"),
            bigquery.SchemaField(name="Inserciones", field_type="INTEGER")
        ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loadgenied data.
        write_disposition="WRITE_APPEND",
    )

    # table_id = "your-project.your_dataset.your_table_name"
    project = "dashboards-competencia"
    cliente = "eDreams"
    table = "kantarpy"
    table_id = project + '.' + cliente + '.' + table

    # upload to bigquery
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()