import numpy as np
import pandas as pd
import re
import shapefile
import datetime
from dateutil.relativedelta import relativedelta
from osgeo import gdal
from shapely.geometry import MultiPoint, Point, Polygon,shape
from shapely.geometry.polygon import Polygon
from streetaddress import StreetAddressFormatter, StreetAddressParser


#----------------------------------------Import option 1: From Blob Storage---------------------------

from azure.storage.blob import BlockBlobService
from io import StringIO
from io import BytesIO
import io

StorageAccountName = XXXXXX
AccountKey = XXXXXX
ContainerName = XXXXX
shapefile_date = XXXXX
Records = XXXXXX
Customers = XXXXX


blob_service = BlockBlobService(account_name = StorageAccountName, account_key = AccountKey)


blobstring_records = blob_service.get_blob_to_text(ContainerName, Records).content
blobstring_customers = blob_service.get_blob_to_text(ContainerName, Customers, encoding='latin-1').content

dt_records3 = pd.read_csv(StringIO(blobstring_records))
customers2 = pd.read_csv(StringIO(blobstring_customers))




from zipfile import ZipFile
import requests

url = '......./data/PLCHC_Adis.zip'

r = requests.get(url)
z = ZipFile(io.BytesIO(r.content))

filenames = [y for y in sorted(z.namelist()) for ending in ['dbf', 'prj', 'shp', 'shx'] if y.endswith(ending)]
print(filenames)


z.extractall()
dbf, prj, shp, shx = [filename for filename in filenames]
shapefile2 = shapefile.Reader(shp)



#------------------------------------------------Import option 2: From local------------------------------
# Load shapefile
shapefile1 = shapefile.Reader(r"....\PLCHC_Adis.shp")
all_shapes = shapefile1.shapes()   # Get shapes
all_records = shapefile1.records()  # Get records
shape(all_shapes[0])  # Visualize

# load records file
dt_records = pd.read_csv(r".....\hamilton.csv")
dt_records['Coordinates'] = list(zip(dt_records.LON, dt_records.LAT))     # Combine longitude and latitude    
dt_records['UNIT'] = dt_records['UNIT'].replace(np.nan, '', regex=True)       # Remove NaN string from this column
dt_records['Address'] = dt_records.NUMBER.map(int).map(str) + " " + dt_records['STREET'] + " " + dt_records['UNIT'].map(str)# Combine Street, unit, city 

dt_records2 = pd.read_csv(r".....\hamilton2.csv")
dt_records2['Address'] = dt_records2['Address'].apply(lambda x: str(x).upper())
dt_records2['StandardCity'] = dt_records2['StandardCity'].apply(lambda x: str(x).upper())
dt_records2['Coordinates'] = list(zip(dt_records2.Longitude, dt_records2.Latitude))     # Combine longitude and latitude    

# load customers table from database
customers = pd.read_csv(r".....\Customers.csv", encoding='latin-1')
customers = customers.rename(columns={'StandardAddress': 'Address'})   # Rename column
customers['LastActivityDate'] = pd.to_datetime(customers['LastActivityDate']) # Convert column to datetime format
customers = customers.sort_values('LastActivityDate').groupby('Address').tail(1)
customers['isActive'] = customers['LastActivityDate'].apply(lambda x: 'True' if (datetime.datetime.today()-x).days<=365 else 'False')
customers = customers.reset_index()
customers = customers.drop('index', axis=1)




#----------------------------------------Pre-processing-------------------------
def splitWrongTH(s):
    ''' return splited letters + number
    
    Split the items in list
    Split the items with number and strings in list, for example: W12 to W 12
    
    '''
    return filter(None, re.split(r'(\d+)', s))

def USAddressFormatter_TH(row):
    ''' return correct formatt
    
    format the wrong address with combination of direction and number, for example: W12 TH to W 12TH
    '''        
    if 'TH' in row.split():     # look for the address that contains 'TH'

        list_formatting = []

        for i in splitWrongTH(row):        # splitWrongTH outputs [' 217', ' W', '12', 'TH', 'ST']
            i = i.strip()                # remove spaces
            list_formatting.append(i)    # create list as ['217', 'W', '12', 'TH ST']

        list_formatting2 = ' '.join(str(r) for r in list_formatting)    # create list as ['217 W 12 TH ST']
        list_formatting2 = list_formatting2.split()    # create list as ['217', 'W', '12', 'TH', 'ST']

        
        if 'TH' in list_formatting2:
            idx = list_formatting2.index('TH')
            num_idx = idx - 1
            num = list_formatting2[num_idx]

            if num.isdigit():           # make sure the item in the left side of 'TH' is digit
                list_formatting2[num_idx:idx+1] = [''.join(list_formatting2[num_idx:idx+1])]
                list_formatting3 = ' '.join(str(r) for r in list_formatting2)
                row = list_formatting3
                return row

            else:
                return row

        else:
            return row
    
    else:
        return row
    

    
def USAddressFormatter_Symbol(row):   
    
    row_split = row.split()

    if '&' in row_split:
        row_item_idx = row_split.index('&')

        if row_split[-1] == row_split[row_item_idx]:
            row_split_new = row_split[:-1]

            if '#' in row_split_new:
                row_item_idx2 = row_split_new.index('#')

                if row_split_new[-1] == row_split_new[row_item_idx2]:
                    row_split_new2 = row_split_new[:-1]
                    row_split_new2_output = ' '.join(str(rr) for rr in row_split_new2)                       
                    row = row_split_new2_output
                    return row

                else:
                    row_split_new3_output = ' '.join(str(rr) for rr in row_split_new)
                    row = row_split_new3_output
                    return row

            else:
                row_split_new_output = ' '.join(str(r) for r in row_split_new)
                row = row_split_new_output
                return row

        else:
            return row

    else:
        return row



def USAddressFormatter_Abbr(row):
    if 'AV' in row.split():
        row = row.replace('AV', 'AVE')
        return row

    if 'CR' in row.split():
        row = row.replace('CR', 'CIR')  
        return row

    if 'AL' in row.split():
        row = row.replace('AL', 'ALY')
        return row

    else:
        return row
        
        
        
        
 
 
 
 
 def PreProcessing_records(dt):
    '''
    return corect formatt
    
    ''' 
    dt.Address = dt.Address.apply(lambda x: str(x).upper())    # uppercase
    dt.Address = dt.Address.str.rstrip()     # Remove spaces 
    
    dt_Address_ar = np.asarray(dt.Address)
    vfunc_TH = np.vectorize(USAddressFormatter_TH)        # Custome formatter, correct wrong combination of number + TH
    dt_Address = vfunc_TH(dt_Address_ar)
    dt_Address_list = dt_Address.tolist()
    dt['Address'] = dt_Address_list
    dt['Address'] = dt.Address.str.rstrip()

    dt_Address_ar2 = np.asarray(dt.Address)    
    vfunc_Abbr = np.vectorize(USAddressFormatter_Abbr)    # Custom formatter, replace wrong abbreviation
    dt_Address2 = vfunc_Abbr(dt_Address_ar2)
    dt_Address2_list = dt_Address2.tolist()
    dt['Address'] = dt_Address2_list
    dt['Address'] = dt.Address.str.rstrip()
    
    dt_Address_ar3 = np.asarray(dt.Address)
    vfunc_Sym = np.vectorize(USAddressFormatter_Symbol)   # Customer formatter, remove useless symbols
    dt_Address3 = vfunc_Sym(dt_Address_ar3)
    dt_Address3_list = dt_Address3.tolist()
    dt['Address'] = dt_Address3_list
    dt['Address'] = dt.Address.str.rstrip()
    
    abbr_formatter = StreetAddressFormatter()   # load python package 'Formatter'
    for item in dt.Address:
        item = abbr_formatter.abbrev_direction(item)
        item = abbr_formatter.abbrev_street_avenue_etc(item)
    
    dt.Address = dt.Address.apply(lambda x: str(x).upper())  # uppercase again
    
    return dt
    
    
    
    
    
    
    
    boundary_list = []
for n in range(len(all_shapes)):
    if all_records[n][1] == 'Walnut Hills':
        boundary_list.append(all_shapes[n])
    if all_records[n][1] == 'Bond Hill':
        boundary_list.append(all_shapes[n])
    if all_records[n][1] == 'Price Hill':
        boundary_list.append(all_shapes[n])

records_list = []
for m in range(len(all_records)):
    if all_records[m][1] == 'Walnut Hills':
        records_list.append(all_records[m][1])
    if all_records[m][1] == 'Bond Hill':
        records_list.append(all_records[m][1])
    if all_records[m][1] == 'Price Hill':
        records_list.append(all_records[m][1])

def find_coordinates(row):
    for i in range(len(boundary_list)):
        if Point(row).within(shape(boundary_list[i])):
            return records_list[i]
        
def find_coordinates_results(dt):
    dt_coor_ar = np.asarray(dt.Coordinates)
    vfunc_isCoor = np.vectorize(find_coordinates)
    
    dt_coor = vfunc_isCoor(dt_coor_ar)
    dt_coor_list = dt_coor.tolist()
    
    return dt_coor_list


def split_Customer(dt):
    isnotCus = dt[dt['isCustomer']==False]
    isCus = dt[dt['isCustomer']==True]
    return isnotCus, isCus


def isActiveCus(row):
    if list(cus_Clean[cus_Clean['Address']==row]['isActive'])[0] == 'True':
        return 'True'
    else:
        return 'False'
    
def find_isActiveCustomer(dt):
    
    dt_active_ar = np.asarray(dt.Address)
    vfunc_isCus = np.vectorize(isActiveCus)
    
    dt_active = vfunc_isCus(dt_active_ar)
    dt_active_list = dt_active.tolist()
    
    return dt_active_list
    
    
    
    
    
    
    
    
dt_Clean = PreProcessing_records(dt_records2)   # Clean records
cus_Clean = PreProcessing_records(customers)   # Clean customers


dt_Clean['Location'] = find_coordinates_results(dt_Clean)
dt_Clean = dt_Clean[dt_Clean['Location'].notnull()]
dt_Clean['isCustomer'] = dt_Clean['Address'].isin(cus_Clean.Address)   # Match to see if records is Customers or not

dt_Clean_isnotCus, dt_Clean_isCus = split_Customer(dt_Clean)     # Split isCustomers and isnotCustomers

dt_Clean_isnotCus['isActiveCustomer'] = 'False'                             # Return 'False' to those 'is not Customer'

dt_Clean_isCus['isActiveCustomer'] = find_isActiveCustomer(dt_Clean_isCus)

dt_Clean_new = pd.concat([dt_Clean_isnotCus, dt_Clean_isCus])
dt_Clean_new = dt_Clean_new.sort_index()


dt_Clean_new = dt_Clean_new.reset_index()
dt_Clean_new.drop('index', axis = 1)


# Ouput
dt_Clean_new.to_csv('Hamilton_ADI.csv')
