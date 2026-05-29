from odp.lib.client import ODPClient
from etc.config import config
from somisana.const import SOMISANAScope

ocean_dataset_api_url = config['OCEAN_DATASET']['API_URL']
hydra_url = config['OCEAN_DATASET']['HYDRA_PUBLIC_URL']
client_id = config['OCEAN_DATASET']['SOMISANA_CATALOG_CI_CLIENT_ID']
client_secret = config['OCEAN_DATASET']['SOMISANA_CATALOG_CI_CLIENT_SECRET']

scopes = [
    SOMISANAScope.PRODUCT_READ.value,
    SOMISANAScope.DATASET_READ.value,
    SOMISANAScope.RESOURCE_READ.value
]

cli = ODPClient(ocean_dataset_api_url, hydra_url, client_id, client_secret, scopes)
