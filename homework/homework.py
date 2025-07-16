"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import zipfile
import pandas as pd

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    
    # Crear directorio de salida
    output_dir = 'files/output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Lista para almacenar DataFrames
    dataframes = []
    
    # Procesar archivos comprimidos
    input_files = [f'files/input/bank-marketing-campaing-{n}.csv.zip' for n in range(10)]
    
    for zip_file in input_files:
        with zipfile.ZipFile(zip_file, 'r') as zf:
            filename = zf.namelist()[0]
            
            with zf.open(filename) as file:
                df = pd.read_csv(file)
                dataframes.append(df)
    
    # Consolidar datos
    consolidated_df = pd.concat(dataframes, ignore_index=True)
    
    # Procesar archivo client.csv
    client_df = consolidated_df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    
    # Transformaciones para job
    client_df['job'] = client_df['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    
    # Transformaciones para education
    client_df['education'] = client_df['education'].str.replace('.', '_', regex=False)
    client_df.loc[client_df['education'] == 'unknown', 'education'] = pd.NA
    
    # Conversiones binarias
    client_df['credit_default'] = client_df['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
    client_df['mortgage'] = client_df['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)
    
    # Guardar client.csv
    client_df.to_csv(f'{output_dir}/client.csv', index=False)
    
    # Procesar archivo campaign.csv
    campaign_cols = ['client_id', 'number_contacts', 'contact_duration', 
                    'previous_campaign_contacts', 'previous_outcome', 
                    'campaign_outcome', 'day', 'month']
    campaign_df = consolidated_df[campaign_cols].copy()
    
    # Conversiones de resultado
    campaign_df['previous_outcome'] = campaign_df['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
    campaign_df['campaign_outcome'] = campaign_df['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)
    
    # Crear fecha de contacto
    month_mapping = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08', 
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }
    
    campaign_df['last_contact_date'] = (
        '2022-' + 
        campaign_df['month'].map(month_mapping) + 
        '-' + 
        campaign_df['day'].astype(str).str.pad(2, fillchar='0')
    )
    
    # Remover columnas temporales
    campaign_df.drop(['day', 'month'], axis=1, inplace=True)
    
    # Guardar campaign.csv
    campaign_df.to_csv(f'{output_dir}/campaign.csv', index=False)
    
    # Procesar archivo economics.csv
    economics_df = consolidated_df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    
    # Guardar economics.csv
    economics_df.to_csv(f'{output_dir}/economics.csv', index=False)


if __name__ == "__main__":
    clean_campaign_data()
