import os
import time
import shutil
import pysftp
from datetime import date, datetime


# ------------------------------------------------------------------------------
# Connects To STFP
# ------------------------------------------------------------------------------


def connect_sftp():
    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        sftp = pysftp.Connection(host='host',
                                 username='user',
                                 password='password',
                                 cnopts=cnopts)
        return sftp
    except Exception as e:
        print(f"Failed to connect to SFTP server: {e}")
        return None


# ------------------------------------------------------------------------------
# STFP File Move
# ------------------------------------------------------------------------------


# Move files from SFTP to the specified path
def move_files(sftp):
    local_path = r'local_path'  # Replace with the local path
    print(local_path)
    # Get the list of files in the remote directory
    files = sftp.listdir()

    sftp.get_d(sftp.getcwd(), local_path)
    time.sleep(5)
    for file in files:
        sftp.remove(file)

    print('Files moved successfully')
    os.chdir(local_path)

    return

# ------------------------------------------------------------------------------
# STFP File Transfer
# ------------------------------------------------------------------------------


def file_transfer():
    with connect_sftp() as sftp:
        print(sftp.getcwd())
        print(sftp.listdir())
        move_files(sftp)
        sftp.close()
        sort_files()
        print(os.listdir(os.getcwd()))

# ------------------------------------------------------------------------------
# STFP File Sort
# ------------------------------------------------------------------------------


def sort_files():
    # Define the local path where the PDF files are located
    local_path = r'local_path'
    
    # Get the list of files in the directory
    dir_list = os.listdir(local_path)
    
    # Iterate through each file
    for file in dir_list:
        # Check if the file is a PDF
        if file.endswith('.PDF'):
            try:
                # Extract the date from the file name
                file_name = file.split('-')[1].split('.')[0]
                file_date = datetime.strptime(file_name, '%Y%m%d').date()
                year = file_date.year
                month = file_date.month
                
                # Construct the destination folders
                year_folder = os.path.join(local_path, str(year))
                month_folder = os.path.join(year_folder, str(month))
                
                # Create the year and month folders if they don't exist
                if not os.path.exists(year_folder):
                    os.mkdir(year_folder)
                if not os.path.exists(month_folder):
                    os.mkdir(month_folder)
                
                # Move the file to the appropriate month folder
                shutil.move(os.path.join(local_path, file), month_folder)
                
            except Exception as e:
                print(f"Error processing file '{file}': {e}")
    os.chdir(local_path)


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------


def main():
    today = date.today()
    day = today.day
    month = today.month
    year = today.year
    print(month, day, year, sep='-')
    print('--------------------------------------------------------------------')
    print('Connecting to SFTP')
    file_transfer()
    print('--------------------------------------------------------------------')
    print('Done')


# ------------------------------------------------------------------------------
# init
# ------------------------------------------------------------------------------


if __name__ == '__main__':
    main()
