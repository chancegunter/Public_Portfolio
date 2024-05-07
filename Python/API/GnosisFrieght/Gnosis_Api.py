import os
import sys
import csv
import requests
import pandas as pd
import numpy as np
import mysql.connector
from datetime import date, timedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

cur_dir = Path(__file__).parent
sys.path.append(str(cur_dir / '../../../../../../Credentials'))
import credentials

url = "https://api.freight.fyi/api/"


# ------------------------------------------------------------------------------
# Connects To The Read DB
# ------------------------------------------------------------------------------


def connect_read():
    conn = mysql.connector.connect(host=credentials.host_read,
                                   database=credentials.database_hq,
                                   user=credentials.username,
                                   password=credentials.password,
                                   ssl_disabled=True)

    return conn


# ------------------------------------------------------------------------------
# Connects to API AUTH
# ------------------------------------------------------------------------------


def auth():
    authurl = url + 'auth/token'

    payload = {
        'username': credentials.gnosis_username,
        'password': credentials.gnosis_password
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", authurl, headers=headers, data=payload)
    auth_tkn = response.json()['access_token']

    print(response.json())

    return auth_tkn


# ------------------------------------------------------------------------------
# Get Post Query
# ------------------------------------------------------------------------------


def get_post_data(conn_read, original_path):
    os.chdir(original_path)
    os.chdir('../Queries')
    folder = os.path.abspath(os.curdir)
    with open(folder + '/POST_Update_query.sql', 'r') as data_query:
        container_df = pd.read_sql_query(data_query.read(), conn_read)

    return container_df


# ------------------------------------------------------------------------------
# POST with Scac and w/o Scac
# ------------------------------------------------------------------------------


def postings(container_df, auth_tkn):
    try:
        url = f'https://api.freight.fyi/api/v1/tracking_requests/'
        url_scac = url + 'with_carrier'

        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {auth_tkn}',
            'Content-Type': 'application/json'
        }
        payload = []

        container_scac_df = container_df[(~container_df['opo_container_carrier'].isnull())].drop_duplicates()
        container_noscac_df = container_df[(container_df['opo_container_carrier'].isnull())].drop_duplicates()

        scac_list = container_scac_df.to_dict('records')

        for container in scac_list:
            container_dict = {
                "mbl_number": f'{container["opo_container_hbl_number"]}',
                "carrier_scac": f'{container["opo_container_carrier"]}'
            }
            payload.append(container_dict)

        response = requests.post(url_scac, headers=headers, json=payload)
        resp_scac = response.json()
        answers = []

        for d in resp_scac.keys():
            resp_scac[d].update({"mbl_number": f"{str(d)}"})
            answers.append(resp_scac[d])

        with_scac = pd.DataFrame.from_records(answers)

    except Exception as e:
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        print(e)
        quit()

    try:
        url = f'https://api.freight.fyi/api/v1/tracking_requests/'

        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {auth_tkn}',
            'Content-Type': 'application/json'
        }
        container_noscac_lst = container_noscac_df["opo_container_hbl_number"].values.T.tolist()

        payload = {
            "mbl_numbers": container_noscac_lst
        }

        response = requests.post(url, headers=headers, json=payload)
        resp_scac = response.json()
        answers = []

        for d in resp_scac.keys():
            resp_scac[d].update({"mbl_number": f"{str(d)}"})
            answers.append(resp_scac[d])

        without_scac = pd.DataFrame.from_records(answers)

    except Exception as e:
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        print(e)
        quit()

    return with_scac, without_scac


# ------------------------------------------------------------------------------
# Get Containers and Active
# ------------------------------------------------------------------------------


def containers(auth_tkn):
    act_lst = []
    header = []
    i = 0
    worth = True
    while worth:
        try:
            url = f'https://api.freight.fyi/api/v1/containers/active/?page_num={i}&page_size=50'

            payload = {}
            headers = {
                'Authorization': f'Bearer {auth_tkn}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            response = requests.request("GET", url, headers=headers, data=payload, timeout=45)
            shipments = response.json()

            if len(header) == 0:
                header = [key for key in response.json()['containers'][0]]
            else:
                pass
            for cargo in shipments['containers']:
                act_lst.append(cargo)
            print(shipments['metadata'])
            if i == shipments['metadata']['last_page']:
                break
            i += 1

        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
            print(e)
            break

    container_lst = []
    i = 0
    worth = True
    while worth:
        try:
            url = f'https://api.freight.fyi/api/v1/containers/?page_num={i}&page_size=50'

            payload = {}
            headers = {
                'Authorization': f'Bearer {auth_tkn}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            response = requests.request("GET", url, headers=headers, data=payload, timeout=45)
            shipments = response.json()

            if len(header) == 0:
                header = [key for key in response.json()['containers'][0]]
            else:
                pass
            for cargo in shipments['containers']:
                container_lst.append(cargo)
            print(shipments['metadata'])
            if i == shipments['metadata']['last_page']:
                break
            i += 1

        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
            break

    downloads_path = str(Path.home() / "Downloads")
    os.chdir(downloads_path)

    container_df = pd.DataFrame.from_records(container_lst)
    act_df = pd.DataFrame.from_records(act_lst)

    return container_df, act_df


# ------------------------------------------------------------------------------
# Queries for Exception Data
# ------------------------------------------------------------------------------


def checker(container_df, conn_read):
    container_num = container_df[['container_number']].drop_duplicates()
    container_lst = container_num.astype(str).values.T.tolist()
    container_constraint = '\'' + '\', \''.join(container_lst[0]) + '\''

    query = f"""
            
    """

    checked = pd.read_sql(query, conn_read)

    return checked


# ------------------------------------------------------------------------------
# read in if making quick edits
# ------------------------------------------------------------------------------


def readin(original_path, today_date):
    os.chdir(original_path)
    folder = r'folder'
    os.chdir(folder)
    file_name = 'API_Results' + today_date.strftime("%d%m%Y") + '.xlsx'

    sheet = [{'Name': 'POST_Scac', 'df': 'with_scac'},
             {'Name': 'POST', 'df': 'without_scac'},
             {'Name': 'Containers', 'df': 'container_df'},
             {'Name': 'Active', 'df': 'act_df'}]

    with_scac = pd.read_excel(folder + file_name, sheet_name=sheet[0]['Name'])
    without_scac = pd.read_excel(folder + file_name, sheet_name=sheet[1]['Name'])
    container_df = pd.read_excel(folder + file_name, sheet_name=sheet[2]['Name'])
    act_df = pd.read_excel(folder + file_name, sheet_name=sheet[3]['Name'])

    return with_scac, without_scac, container_df, act_df


# ------------------------------------------------------------------------------
# Manipulates the data
# ------------------------------------------------------------------------------


def manipulations(container_df, checked_df, today_date):
    container_df = container_df[['container_number', 'container_journey_start_key', 'ocean_carrier_scac',
                                 'mother_vessel', 'mother_voyage', 'pol_city', 'pod_city', 'final_dest_city',
                                 'loaded_on_vessel_dt', 'vessel_atd_dt', 'discharged_dt', 'rail_departed_dt',
                                 'rail_ata_dt', 'rail_eta_dt', 'gnosis_rail_eta_dt', 'vessel_eta_dt',
                                 'gnosis_estimated_discharge_dt', 'last_free_demurrage_day_dt',
                                 'gnosis_estimated_last_free_demurrage_day_dt', 'last_free_detention_day_dt',
                                 'gnosis_estimated_last_free_detention_day_dt', 'out_gate_dt', 'empty_returned_dt',
                                 'customs_clearance_dt', 'carrier_release_dt']]

    # replacing nan with non
    container_df = container_df.replace({np.nan: None})

    # Splitting out the MBL from the container number in Journey Key
    container_df['container_journey_start_key'] = pd.Series(
        container_df['container_journey_start_key']).str.split('_').str[1]

    # Converting Data to Date
    container_df['rail_eta_dt'] = pd.to_datetime(container_df['rail_eta_dt']).dt.date
    container_df['vessel_eta_dt'] = pd.to_datetime(container_df['vessel_eta_dt']).dt.date
    container_df['loaded_on_vessel_dt'] = pd.to_datetime(container_df['loaded_on_vessel_dt']).dt.date
    container_df['vessel_atd_dt'] = pd.to_datetime(container_df['vessel_atd_dt']).dt.date
    container_df['discharged_dt'] = pd.to_datetime(container_df['discharged_dt']).dt.date
    container_df['rail_departed_dt'] = pd.to_datetime(container_df['rail_departed_dt']).dt.date
    container_df['rail_ata_dt'] = pd.to_datetime(container_df['rail_ata_dt']).dt.date
    container_df['gnosis_rail_eta_dt'] = pd.to_datetime(container_df['gnosis_rail_eta_dt']).dt.date
    container_df['gnosis_estimated_discharge_dt'] = pd.to_datetime(
        container_df['gnosis_estimated_discharge_dt']).dt.date
    container_df['last_free_demurrage_day_dt'] = pd.to_datetime(container_df['last_free_demurrage_day_dt']).dt.date
    container_df['gnosis_estimated_last_free_demurrage_day_dt'] = pd.to_datetime(
        container_df['gnosis_estimated_last_free_demurrage_day_dt']).dt.date
    container_df['last_free_detention_day_dt'] = pd.to_datetime(container_df['last_free_detention_day_dt']).dt.date
    container_df['gnosis_estimated_last_free_detention_day_dt'] = pd.to_datetime(
        container_df['gnosis_estimated_last_free_detention_day_dt']).dt.date
    container_df['out_gate_dt'] = pd.to_datetime(container_df['out_gate_dt']).dt.date
    container_df['empty_returned_dt'] = pd.to_datetime(container_df['empty_returned_dt']).dt.date
    container_df['customs_clearance_dt'] = pd.to_datetime(container_df['customs_clearance_dt']).dt.date
    container_df['carrier_release_dt'] = pd.to_datetime(container_df['carrier_release_dt']).dt.date

    # Condition & Value logic for particular columns (vessel_atd, demurrage_day, detention_day, ramp_eta)

    conditions = [
        container_df['loaded_on_vessel_dt'].isna(),
        container_df['loaded_on_vessel_dt'].notna()
    ]
    values = [
        container_df['vessel_atd_dt'],
        container_df['loaded_on_vessel_dt']
    ]
    container_df['vessel_atd'] = np.select(conditions, values)

    conditions = [
        container_df['last_free_demurrage_day_dt'].isna(),
        container_df['last_free_demurrage_day_dt'].notna()
    ]
    values = [
        container_df['gnosis_estimated_last_free_demurrage_day_dt'],
        container_df['last_free_demurrage_day_dt']
    ]
    container_df['demurrage_day'] = np.select(conditions, values)

    conditions = [
        container_df['last_free_detention_day_dt'].isna(),
        container_df['last_free_detention_day_dt'].notna()
    ]
    values = [
        container_df['gnosis_estimated_last_free_detention_day_dt'],
        container_df['last_free_detention_day_dt']
    ]
    container_df['detention_day'] = np.select(conditions, values)

    conditions = [container_df['rail_ata_dt'].notna(),
                  container_df['rail_ata_dt'].isnull() & container_df['rail_eta_dt'].notna(),
                  container_df['discharged_dt'].notna(),
                  container_df['vessel_eta_dt'].notna(),
                  container_df['gnosis_estimated_discharge_dt'].notna(),
                  container_df[['rail_ata_dt', 'rail_eta_dt', 'discharged_dt', 'vessel_eta_dt']].isna().all(axis=1)
                  ]
    values = [container_df['rail_ata_dt'],
              container_df['rail_eta_dt'],
              container_df['discharged_dt'],
              container_df['vessel_eta_dt'],
              container_df['gnosis_estimated_discharge_dt'],
              pd.NaT  # default value for when all conditions are False
              ]

    container_df['ramp_eta'] = np.select(conditions, values)
    container_df['eta'] = container_df['ramp_eta'] + timedelta(days=5)
    # container_df['ramp_eta'] = np.where(container_df['ramp_eta'] >= today_date,
    #                                     container_df['ramp_eta'], np.nan)
    # container_df['eta'] = np.where(container_df['ramp_eta'] >= today_date,
    #                                container_df['eta'], np.nan)

    # Rename Columns prior to the merge with hq
    container_df.rename(columns={
        'container_journey_start_key': 'mbl_number',
        'ocean_carrier_scac': 'gnosis_scac',
        'mother_vessel': 'gnosis_mother_vessel',
        'mother_voyage': 'gnosis_mother_voyage',
        'pol_city': 'gnosis_origin_port',
        'pod_city': 'gnosis_pod_city',
        'final_dest_city': 'gnosis_final_discharge',
        'vessel_atd': 'gnosis_vessel_atd',
        'discharged_dt': 'gnosis_discharged_dt',
        'rail_departed_dt': 'gnosis_rail_departed_dt',
        'ramp_eta': 'gnosis_ramp_eta',
        'eta': 'gnosis_eta',
        'demurrage_day': 'gnosis_demurrage_day',
        'out_gate_dt': 'gnosis_out_gate',
        'detention_day': 'gnosis_detention_day',
        'empty_returned_dt': 'gnosis_empty_returned',
        'customs_clearance_dt': 'gnosis_customs_clearance',
        'carrier_release_dt': 'gnosis_carrier_release'
    }, inplace=True)

    container_df = container_df[['container_number', 'mbl_number', 'gnosis_scac', 'gnosis_mother_vessel',
                                 'gnosis_mother_voyage', 'gnosis_origin_port', 'gnosis_pod_city',
                                 'gnosis_final_discharge', 'gnosis_vessel_atd', 'gnosis_discharged_dt',
                                 'gnosis_rail_departed_dt', 'gnosis_ramp_eta', 'gnosis_eta', 'gnosis_demurrage_day',
                                 'gnosis_out_gate', 'gnosis_detention_day', 'gnosis_empty_returned',
                                 'gnosis_customs_clearance', 'gnosis_carrier_release']]

    # Merging with HQ exception Data
    upload_df = pd.merge(container_df,
                         checked_df,
                         on=['container_number', 'mbl_number'],
                         how='left')

    upload_df = upload_df.replace({np.nan: None})
    upload_df['exception'] = None
    upload_df['notes'] = None
    upload_df['dropper'] = None

    return upload_df

# ------------------------------------------------------------------------------
# Match vs Mismatch
# ------------------------------------------------------------------------------

def is_mismatch(x):
    mismatch = []
    for o in range(len(excpt_cols)):
        gnosis_col = 'gnosis_' + excpt_cols[o]
        hq_col = 'hq_' + excpt_cols[o]
        if (excpt_cols[o] in date_fields) and (not pd.isnull(x[gnosis_col])):
            if pd.isnull(x[hq_col]) or (x[gnosis_col] < x[hq_col]):
                mismatch.append(x[gnosis_col])
            else:
                mismatch.append(None)
        elif pd.isnull(x[gnosis_col]) and pd.isnull(x[hq_col]):
            mismatch.append(None)
        elif x[gnosis_col] != x[hq_col]:
            mismatch.append('mismatch')
        else:
            mismatch.append(None)

    return pd.Series(mismatch)


def is_match(x):
    match = []
    for e in range(len(cols)):
        gnosis_col = 'gnosis_' + cols[e]
        hq_col = 'hq_' + cols[e]
        if pd.isnull(x[hq_col]) or pd.isna(x[hq_col]):
            match.append(x[gnosis_col])
        elif x[hq_col] is not None and x[gnosis_col] is not None and x[hq_col] != x[gnosis_col]:
            match.append(x[gnosis_col])
        else:
            match.append(None)

    return pd.Series(match)

# ------------------------------------------------------------------------------
# Connects To The DEV DB
# ------------------------------------------------------------------------------


def prep_upload(upload_df, today_date):
    complete_df = upload_df.copy()
    date_cols = ['hq_vessel_atd', 'hq_discharged_dt', 'hq_rail_departed_dt', 'hq_ramp_eta', 'hq_eta',
                 'hq_demurrage_day', 'hq_out_gate', 'hq_detention_day', 'hq_empty_returned', 'hq_customs_clearance',
                 'hq_carrier_release']

    year_from = today_date + relativedelta(years=1)
    year_ago = today_date - relativedelta(years=1)


    for column in date_cols:
        complete_df[column] = complete_df[column].apply(lambda val: None if val is None or pd.isna(val)
                                                        else datetime.strptime(str(val), "%Y-%m-%d") if pd.notna(val)
                                                        else None)
        # Convert valid dates, handle None and NaN
        complete_df[column] = pd.to_datetime(complete_df[column], errors='coerce')
        # 'coerce' invalid dates to NaT
        complete_df[column] = complete_df[column].dt.date

    returned_df = complete_df[(complete_df['hq_empty_returned'].notna())]
    complete_df = complete_df[complete_df['hq_empty_returned'].isna()]

    hq_list = [col for col in complete_df if col.startswith('hq_')]
    gnosis_list = [col for col in complete_df if col.startswith('gnosis_')]
    gnosis_list.remove('gnosis_scac')

    # ------------------------------------------------------------------------------
    # Mismatch Check
    # ------------------------------------------------------------------------------

    complete_df['exception'] = np.where(complete_df[hq_list[1:]].isna().all(1), 1, 0)
    complete_df['dropper'] = np.where(complete_df[gnosis_list[1:]].isna().all(1), 1, 0)
    complete_df['dropper'] = np.where(complete_df[hq_list[1:]].isna().all(1), 1, 0)
    complete_df['dropper'] = np.where(complete_df[gnosis_list[1:]].isna().all(1), 2, complete_df['dropper'])
    complete_df['count'] = complete_df.groupby('container_number')['container_number'].transform('size')

    condition = [(complete_df['exception'] == 1) | (complete_df['dropper'] == 1)]
    condition = [(complete_df['exception'] == 1) | (complete_df['dropper'] >= 1)]
    value = [1]
    complete_df['exception'] = np.select(condition, value)

    # ------------------------------------------------------------------------------
    # Mismatch Check
    # ------------------------------------------------------------------------------

    cols = [
        'scac', 'origin_port', 'mother_vessel', 'mother_voyage', 'pod_city', 'final_discharge',
        'ramp_eta', 'eta', 'customs_clearance', 'carrier_release', 'vessel_atd',
        'discharged_dt', 'rail_departed_dt', 'out_gate', 'empty_returned'
    ]

    excpt_cols = [
        'demurrage_day', 'detention_day'
    ]

    date_fields = [
        'demurrage_day', 'detention_day',
    ]

    complete_df[cols] = complete_df.apply(is_match, axis=1)
    complete_df[excpt_cols] = complete_df.apply(is_mismatch, axis=1)

    arr = np.where(complete_df.eq('mismatch'), complete_df.columns + ', ', '').sum(axis=1)
    arr = list(arr)

    complete_df['notes'] = arr
    complete_df['notes'] = np.where(complete_df['notes'] == '', None, complete_df['notes'])
    complete_df['exception'] = np.where(complete_df['notes'].notna(), 1, complete_df['exception'])

    complete_df = complete_df[complete_df['dropper'] < 2]

    exceptions = complete_df[(complete_df['exception'] == 1)]

    complete_df.rename(columns={
        'scac': 'Steamship Line',
        'mother_vessel': 'Vessel',
        'mother_voyage': 'Voyage',
        'origin_port': 'Origin Port',
        'pod_city': 'Discharge Port',
        'final_discharge': 'Final Discharge',
        'vessel_atd': 'Actual Ship Date',
        'discharged_dt': 'Discharge Date',
        'rail_departed_dt': 'Loaded On Rail',
        'ramp_eta': 'ETA_Ramp',
        'origin_port': 'Origin Port',
        'pod_city': 'Discharge Port',
        'final_discharge': 'Final Discharge',
        'vessel_atd': 'Actual Ship Date',
        'discharged_dt': 'Discharge Date',
        'rail_departed_dt': 'Loaded on Rail',
        'ramp_eta': 'ETA Ramp',
        'eta': 'ETA',
        'demurrage_day': 'Last Free Date Pull',
        'out_gate': 'Date Pulled',
        'detention_day': 'Last Free Date Return',
        'empty_returned': 'Date Returned',
        'customs_clearance': 'Customs Clearance Date',
        'carrier_release': 'BL Release Date'
    }, inplace=True)

    complete_df = complete_df[[
        'hq_container_name', 'container_number', 'mbl_number',
        'Steamship Line', 'Vessel', 'Voyage', 'Origin Port', 'Discharge Port',
        'Final Discharge', 'Actual Ship Date', 'Discharge Date',
        'Loaded on Rail', 'ETA Ramp', 'ETA', 'Last Free Date Pull',
        'Last Free Date Return', 'Date Pulled', 'Date Returned',
        'Customs Clearance Date', 'BL Release Date']]

    complete_df.rename(columns={'hq_container_name': 'PO # -or- Container Name'}, inplace=True)
    complete_df.rename(columns={'container_number': 'Container Number'}, inplace=True)
    complete_df.rename(columns={'mbl_number': 'HBL'}, inplace=True)
    complete_df['ETD'] = None
    # upload_df = upload_df[upload_df['PO # -or- Container Name'].notna()]
    complete_df = complete_df[complete_df['PO # -or- Container Name'].notna()]

    complete_df['Carrier', 'Consignee', 'Manufacturer', 'Req. Ship Date',
                'Cargo Ready Date', 'Status', 'Hot Container', 'Freight Cost',
                'Customer Importer of Record', 'Door', 'Linked Orders',
                'Container Notes', 'Scanner Notes', 'Receipt Date', 'Maersk DIR', 'External PO ID'] = None

    complete_df = complete_df.reindex(columns=[
        'Carrier', 'PO # -or- Container Name', 'Consignee', 'Container Number', 'HBL',
        'Manufacturer', 'Steamship Line', 'Vessel', 'Voyage', 'Origin Port', 'Discharge Port',
        'Final Discharge', 'Req. Ship Date', 'Cargo Ready Date', 'ETD', 'Actual Ship Date', 'Discharge Date',
        'Loaded on Rail', 'ETA Ramp', 'ETA', 'Status', 'Hot Container', 'Last Free Date Pull',
        'Last Free Date Return', 'Date Pulled', 'Date Returned', 'Freight Cost',
        'Customs Clearance Date', 'BL Release Date', 'Customer Importer of Record', 'Door', 'Linked Orders',
        'Container Notes', 'Scanner Notes', 'Receipt Date', 'Maersk DIR', 'External PO ID'])

    check_list = [col for col in complete_df]

    complete_df = complete_df[~(complete_df[check_list[5:]].isna().all(1))]
    complete_df.replace({'mismatch': None})

    print(complete_df.head(20).to_string())

    returned_df = returned_df[['container_number', 'mbl_number', 'gnosis_scac', 'gnosis_mother_vessel',
                               'gnosis_mother_voyage', 'gnosis_origin_port', 'gnosis_pod_city',
                               'gnosis_final_discharge', 'gnosis_vessel_atd', 'gnosis_discharged_dt',
                               'gnosis_rail_departed_dt', 'gnosis_ramp_eta', 'gnosis_eta', 'gnosis_demurrage_day',
                               'gnosis_out_gate', 'gnosis_detention_day', 'gnosis_empty_returned',
                               'gnosis_customs_clearance', 'gnosis_carrier_release', 'hq_container_name', 'hq_scac',
                               'hq_mother_vessel', 'hq_mother_voyage', 'hq_origin_port', 'hq_pod_city',
                               'hq_final_discharge', 'hq_vessel_atd', 'hq_discharged_dt',
                               'hq_rail_departed_dt', 'hq_ramp_eta', 'hq_eta', 'hq_demurrage_day',
                               'hq_out_gate', 'hq_detention_day', 'hq_empty_returned',
                               'hq_customs_clearance', 'hq_carrier_release', 'hq_status_id', 'notes', 'exception']]

    return complete_df, exceptions, returned_df


# ------------------------------------------------------------------------------
# Connects To The DEV DB
# ------------------------------------------------------------------------------


def exporter(original_path, with_scac, without_scac, container_df, act_df,
             upload_df, exception_df, returned_df, today_date):
    os.chdir(original_path)
    folder = r'local_path'
    os.chdir(folder)
    file_name = 'API_Results_' + today_date.strftime("%Y_%m_%d") + '.xlsx'

    sheet = [{'Name': 'POST_Scac', 'df': with_scac, 'max_row': with_scac.shape[0],
              'max_col': (with_scac.shape[1] - 1)},
             {'Name': 'POST', 'df': without_scac, 'max_row': without_scac.shape[0],
              'max_col': (without_scac.shape[1] - 1)},
             {'Name': 'Containers', 'df': container_df, 'max_row': container_df.shape[0],
              'max_col': (container_df.shape[1] - 1)},
             {'Name': 'Active', 'df': act_df, 'max_row': act_df.shape[0],
              'max_col': (act_df.shape[1] - 1)},
             {'Name': 'Upload', 'df': upload_df, 'max_row': upload_df.shape[0],
              'max_col': (upload_df.shape[1] - 1)},
             {'Name': 'Exception', 'df': exception_df, 'max_row': exception_df.shape[0],
              'max_col': (exception_df.shape[1] - 1)},
             {'Name': 'Returned', 'df': returned_df, 'max_row': returned_df.shape[0],
              'max_col': (returned_df.shape[1] - 1)},
             ]

    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')

    # iteration through sheets
    for worksheets in sheet:
        worksheets['df'].to_excel(writer, sheet_name=worksheets['Name'], startrow=1, header=False, index=False)
        worksheet = writer.sheets[worksheets['Name']]
        column_settings = []

        # getting list of headers
        for header in (worksheets['df']).columns:
            column_settings.append({'header': header})

        # setting table conditions
        worksheet.add_table(0, 0, worksheets['max_row'], worksheets['max_col'],
                            {'columns': column_settings,
                             'style': 'Table Style Medium 2'})

        # setting column widths
        for column in worksheets['df']:
            column_width = 20
            col_idx = (worksheets['df']).columns.get_loc(column)
            worksheet.set_column(col_idx, col_idx, column_width)

    writer.close()

    os.chdir(r'\\fileserver\public\Purchasing\Logistics\Gnosis\Upload')
    upload_df.to_csv('Upload Templates-' + today_date.strftime('%#m.%#d') + '.csv', index=False)


# ------------------------------------------------------------------------------
# Connects To The DEV DB
# ------------------------------------------------------------------------------


def task_scheduler_run_stamp(start_path):
    upload_row = ['', '']
    os.chdir(start_path)
    os.chdir('local_path')
    folder = os.path.abspath(os.curdir)
    with open(folder + '/Task_Scheduler_Log.csv', mode='a') as upload_file:
        upload_writer = csv.writer(upload_file, delimiter=',', lineterminator='\n',
                                   quotechar='"', quoting=csv.QUOTE_MINIMAL)
        upload_row[0] = 'Gnosis_API_New'
        upload_row[1] = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
        upload_writer.writerow(upload_row)
    upload_file.close()


# ------------------------------------------------------------------------------
# Connects To The DEV DB
# ------------------------------------------------------------------------------


def main():
    os.chdir('local_path')
    original_path = os.getcwd()
    today_date = date.today()
    conn_read = connect_read()
    container_df = get_post_data(conn_read, original_path)
    auth_tkn = auth()
    with_scac, without_scac = postings(container_df, auth_tkn)
    container_df, act_df = containers(auth_tkn)
    checked_df = checker(container_df, conn_read)
    upload_df = manipulations(act_df, checked_df, today_date)
    complete_df, exception_df, returned_df = prep_upload(upload_df, today_date)
    exporter(original_path, with_scac, without_scac, container_df, act_df,
             complete_df, exception_df, returned_df, today_date)
    task_scheduler_run_stamp(original_path)

    print('Done')


# ------------------------------------------------------------------------------
# Connects To The DEV DB
# ------------------------------------------------------------------------------

if __name__ == '__main__':
    main()
