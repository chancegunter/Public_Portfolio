import os
import sys
import math
import time
import requests
import pandas as pd
import numpy as np
import mysql.connector
import keyring as kr
from datetime import  datetime
from dateutil import relativedelta
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.driver_cache import DriverCacheManager
from selenium.webdriver.chrome.service import Service as ChromeService
from PIL import Image
from io import BytesIO
from pathlib import Path

cur_dir = Path(__file__).parent
sys.path.append('../../../../../../../Credentials')
import connections

cred = kr.get_credential("hq_db", None)

# ------------------------------------------------------------------------------
# Connects To The DEV DB
# ------------------------------------------------------------------------------


def connect_read():
    conn = mysql.connector.connect(host=connections.host_read,
                                   database=connections.database_analytics,
                                   user=cred.username,
                                   password=cred.password,
                                   ssl_disabled=True)

    return conn


# ------------------------------------------------------------------------------
# Connects To The DEV DB
# ------------------------------------------------------------------------------


def connect_write():
    conn = mysql.connector.connect(host=connections.host_write,
                                   database=connections.database_analytics,
                                   user=cred.username,
                                   password=cred.password,
                                   ssl_disabled=True)

    return conn

# ------------------------------------------------------------------------------
# API Gather
# ------------------------------------------------------------------------------


def api_run():
    url = "https://zendesk.com/api/v2/tickets?sort_by=<string>&sort_order=asc"
    
    payload = {}
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Basic',
        'Cookie': 'cookie1'
                  'cookie2'
    }
    
    response = requests.request("GET", url, headers=headers, data=payload)
    
    # Convert response to dataframe
    data = response.json()
    print(math.ceil(int(data['count']) / 100))
    print(url)
    
    df = pd.DataFrame(data['tickets'])
    df = df[['id', 'created_at', 'custom_fields']]
    
    df['created_at'] = pd.to_datetime(df['created_at']).dt.date
    
    # Create an empty dataframe to store the total data
    total_df = pd.DataFrame()
    total_df = pd.concat([total_df, df])
    
    # Iterate through all the pages
    while 'next_page' in data:
        next_url = data['next_page']
        print(next_url)
        if next_url:
            response = requests.request("GET", next_url, headers=headers, data=payload)
            data = response.json()
            df = pd.DataFrame(data['tickets'])
            df = df[['id', 'created_at', 'custom_fields']]
            df['created_at'] = pd.to_datetime(df['created_at']).dt.date
            total_df = pd.concat([total_df, df])
        else:
            break
    
    # print(total_df.head(5))
    total_df['custom_fields'] = total_df['custom_fields'].apply(
        lambda x: [d for d in x if (
                (d['id'] == 5617423873047) or (d['id'] == 4947202158487) or (d['id'] == 5619213702295) or (
                d['id'] == 4959398612631) or (d['id'] == 5619303132439) or (d['id'] == 4947169674135)
                or (d['id'] == 4961815910295))])
    
    # total_df.to_csv('zendesk.csv', index=False)
    # total_df = pd.read_csv('zendesk.csv')
    
    total_df['parent_sku'] = total_df['custom_fields'].apply(
        lambda x: next((d['value'] for d in x if d['id'] == 5617423873047), None))
    total_df['sku'] = total_df['custom_fields'].apply(
        lambda x: next((d['value'] for d in x if d['id'] == 4947202158487), None))
    total_df['solve'] = total_df['custom_fields'].apply(
        lambda x: next((d['value'] for d in x if d['id'] == 5619213702295), None))
    total_df['place_of_purchase'] = total_df['custom_fields'].apply(
        lambda x: next((d['value'] for d in x if d['id'] == 4959398612631), None))
    total_df['channel'] = total_df['custom_fields'].apply(
        lambda x: next((d['value'] for d in x if d['id'] == 5619303132439), None))
    total_df['order_id'] = total_df['custom_fields'].apply(
        lambda x: next((d['value'].split("-")[1] for d in x if
                        d.get('id') == 4947169674135 and d.get('value') and "-" in d['value']), None))
    total_df['order_date'] = total_df['custom_fields'].apply(
        lambda x: next((d['value'] for d in x if d['id'] == 4961815910295), None))

    damg_list = ['replacement_order__replacement_part', 'replacement_order__whole_unit_replacement',
                 '_replacement_order__substitute_product', 'failed_protector_resolutions']
    reso_list = ['3rd_party_resolutions::no_response_from_customer', '3rd_party_resolutions::resolved_by_wayfair',
                 '3rd_party_resolutions__return_or_exchange_with_vendor', '3rd_party_resolutions__partial_refund',
                 '3rd_party_resolutions___new/replacement_shipment', 'found_missing_parts', 'refund_or_credit__check',
                 'refund_or_credit__amazon_gift_card', 'refund_or_credit__paypal', 'refund_or_credit__original_payment',
                 'refund_or_credit__credit', 'customer_resolutions__cleaning_or_other_product_instructions',
                 '3rd_party_resolutions::duplicate_ticket', 'other_resolution_2', 'no_resolution', 'return_or_exchange',
                 'customer_resolutions__resolved_with_troubleshooting', 'orders__cancelled_order',
                 'orders__shipped_original_order']
    
    conditions = [
        total_df['solve'].isna(),
        total_df['solve'].isin(['replacement_order__partial_unit__box_of_']),
        total_df['solve'].isin(reso_list),
        total_df['solve'].isin(damg_list)
    ]
    
    values = [None, 'Missing Parts', '3rd Party Resolution', 'Damaged']
    
    total_df['reason'] = np.select(conditions, values, default=None)
    total_df.to_csv('zendesk21.csv', index=False)

    total_df = pd.read_csv('zendesk21.csv')
    total_df['created_at'] = pd.to_datetime(total_df['created_at'])

    eighteen_month = datetime.today() - relativedelta.relativedelta(months=18)

    total_df = total_df[(~total_df['reason'].isna()) & (~total_df['sku'].isna()) &
                        (total_df['created_at'] >= eighteen_month)]

    total_df['reason'] = np.where(total_df['reason'] == '3rd Party Resolutions', '3rd Party Resolution',
                                  total_df['reason'])
    
    rescrape(total_df)

    return


# ------------------------------------------------------------------------------
# Upload
# ------------------------------------------------------------------------------


def rescrape(total_df):

    prods = total_df[['sku']]
    print(prods)
    print(total_df.to_string())
    external_ids = prods[['sku']].drop_duplicates()
    container_lst = external_ids.astype(str).values.T.tolist()
    container_constraint = '\'' + '\', \''.join(container_lst[0]) + '\''

    conn_read = connect_read()
    query2 = f'''

    '''

    prod_df = pd.read_sql(query2, conn_read)

    tot_df = pd.merge(total_df,
                      prod_df,
                      how='left',
                      on='sku')

    print(tot_df.to_string())

    tot_df['group_id'] = 1
    tot_df['group'] = 'Zendesk'
    tot_df = tot_df[~tot_df['oproduct_id'].isna()]

    query = '''

    '''
    reasons_df = pd.read_sql(query, conn_read)

    tot_df = pd.merge(
        tot_df,
        reasons_df,
        how='left',
        left_on='reason',
        right_on='aqc_reason'
    )
    print(tot_df.columns)

    tot_df['order_date'] = np.where(tot_df['order_date'] == '3023-05-31', '2023-05-31', tot_df['order_date'])
    tot_df['order_date'] = np.where(tot_df['order_date'] == '8202-07-03', '2023-05-31', tot_df['order_date'])
    tot_df['order_date'] = pd.to_datetime(tot_df['order_date'])  # Convert 'order_date' to datetime if needed

    # Create an empty dictionary to store image lists for each ticket ID
    image_dict = {}

    for index, row in tot_df.iterrows():
        try:
            ticket_id = row['id']

            url = f"https://zendesk.com/api/v2/tickets/{ticket_id}/comments?include_inline_images=TRUE"

            payload = {}
            headers = {
                'Accept': 'application/json',
                'Authorization': 'Basic',
                'Cookie': 'cookie1'
                          'cookie2'
            }

            response = requests.request("GET", url, headers=headers, data=payload)
            data = response.json()
            image_lst = []
            for comment in data['comments']:
                for attachment in comment.get("attachments", []):
                    image_lst.append(attachment["content_url"])

            # Add the image list to the dictionary with the ticket ID as the key
            image_dict[ticket_id] = image_lst
        except:
            image_dict[ticket_id] = []


    # Update the 'image_lst' column in the DataFrame using the dictionary
    tot_df['image_lst'] = tot_df['id'].map(image_dict)

    tot_df = tot_df.explode('image_lst')
    tot_df = tot_df.dropna(subset=['image_lst'])
    tot_df.replace({np.NaN: None}, inplace=True)
    tot_df = tot_df[~tot_df['oproduct_id'].isna()]
    tot_df['image_id'] = tot_df['id'].astype(str) + tot_df['group_id'].astype(str)
    photo_df = tot_df[['image_id', 'image_lst']].copy()
    print(photo_df.to_string())
    photo_df = (photo_df.drop_duplicates(subset='image_lst')).reset_index(drop=True)
    tot_df = (tot_df[['id', 'group_id', 'group', 'order_id', 'oproduct_id',
                      'ovendor_id', 'aqc_reason_id', 'order_date', 'image_id']]).drop_duplicates()
    print(tot_df.to_string())

    upload(tot_df, photo_df, conn_write=connect_write())

    for index, row in tot_df.iterrows():
        ticket_id = row['id']
        # Extract year and month from the 'order_date' column
        if pd.isna(row['order_date']):
            ext_year = pd.to_datetime(row['created_at']).year
            ext_month = pd.to_datetime(row['created_at']).month
            base_directory = fr'local_path\{ext_year}\{ext_month}'
        else:
            year = row['order_date'].year
            month = row['order_date'].month
            base_directory = fr'local_path\{year}\{month}'
    
        # Create folder path
        folder_path = os.path.join(base_directory, str(row['id']))
    
        # Check if the folder already exists, if not, create it
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
    
        if ticket_id in image_dict:
            image_lst = image_dict[ticket_id]
            print(image_lst)
    
            for photo in image_lst:
                photo_sku = str(row['sku'])
                count = 0
                if count >= 1:
                    photo_name = f"{str(count)}_{str(photo_sku)}.jpg"
                else:
                    photo_name = f"{str(photo_sku)}.jpg"
                count += 1
    
                # Navigate to the site and get the image
                driver.get(photo)
                time.sleep(2)
                image_element = driver.find_element(By.TAG_NAME, 'img')
                image_data = image_element.screenshot_as_png
                image = Image.open(BytesIO(image_data))
                # Convert the image to RGB mode
                image = image.convert('RGB')
                image_path = os.path.join(folder_path, f"{str(photo_name)}")
    
                # Check if the file already exists in the destination
                if os.path.exists(image_path):
                    print(
                        f'The file {photo_name} already exists in the destination folder for PO Number {str(row["id"])}.')
                else:
                    image.save(image_path, 'JPEG')
    
    return

# ------------------------------------------------------------------------------
# Upload
# ------------------------------------------------------------------------------


def upload(tot_df, photo_df, conn_write):

    query = '''

    '''

    num_rows = len(photo_df)
    print('Number of rows to be uploaded to AQC_DATA:', num_rows)
    first = 0
    if num_rows < 500:
        last = num_rows
    else:
        last = 500
    i = 1

    while last <= num_rows:
        cursor = conn_write.cursor()

        print('iteration', i)
        print(first)
        print(last)

        upload_list_batch = photo_df[first:last]
        print(upload_list_batch)

        cursor.executemany(query, upload_list_batch.to_dict('records'))
        conn_write.commit()
        cursor.close()
        first += 500
        last += 500
        if last > num_rows and last != (num_rows + 500):
            last = num_rows
        i += 1

    query = '''

            '''

    num_rows = len(tot_df)
    print('Number of rows to be uploaded to AQC_DATA:', num_rows)
    first = 0
    if num_rows < 500:
        last = num_rows
    else:
        last = 500
    i = 1

    while last <= num_rows:
        cursor = conn_write.cursor()

        print('iteration', i)
        print(first)
        print(last)

        upload_list_batch = tot_df[first:last]
        print(upload_list_batch.to_string())

        cursor.executemany(query, upload_list_batch.to_dict('records'))
        conn_write.commit()
        cursor.close()
        first += 500
        last += 500
        if last > num_rows and last != (num_rows + 500):
            last = num_rows
        i += 1

    return

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------


def main():
    api_run()
    return


# ------------------------------------------------------------------------------
# INIT
# ------------------------------------------------------------------------------


if __name__ == '__main__':
    main()
