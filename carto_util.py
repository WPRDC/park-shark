from carto.auth import APIKeyAuthClient
from carto.datasets import DatasetManager

from pprint import pprint
import re

def authorize_carto():
    from prime_ckan.carto_credentials import ORGANIZATION, USERNAME, API_KEY
    BASE_URL = "https://{organization}.carto.com/user/{user}/". \
        format(organization=ORGANIZATION,
                       user=USERNAME)
    auth_client = APIKeyAuthClient(api_key=API_KEY, base_url=BASE_URL, organization=ORGANIZATION)
    return auth_client

def send_file_to_carto(filepath):
    auth_client = authorize_carto()
    dataset_manager = DatasetManager(auth_client)
    datasets = dataset_manager.all()

    filename = filepath.split('/')[-1] 
    carto_name = re.sub('.csv$','',filename)
    carto_name = re.sub('[\.\s]','_',carto_name)
    carto_name = carto_name.lower()
    print("Filename transformed to Carto-style table name is '{}'".format(carto_name))

    dataset_names = [d.name for d in datasets]
    for d in datasets:
        if d.name == carto_name:
            d.delete()

    dataset = dataset_manager.create(filepath)
    print("Carto dataset with name '{}' created.".format(carto_name))


#send_file_to_carto('/Users/drw/test_carto.3.csv')
