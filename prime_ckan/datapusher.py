import requests
import json
from datetime import datetime


class Datapusher:
    """Connection to ckan datastore"""

    def __init__(self, global_settings, server="Staging"):
        self.ckan_url = global_settings['URLs'][server]['CKAN']
        self.dump_url = global_settings['URLs'][server]['Dump']
        self.key = global_settings['API Keys'][server]

    def resource_exists(self, packageid, resource_name):
        """
        Searches for resource on ckan instance
        :param packageid: id of resources parent dataset
        :param resource_name:  resources name
        :return: true if found false otherwise
        """

        check_resource = requests.post(
            self.ckan_url + 'action/package_show',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'id': packageid
            })
        )
        check = json.loads(check_resource.content)

        # If no resources at all
        if len(check['result']['resources']) == 0:
            return False

        # Check if this month's resource already exists
        for resource in check['result']['resources']:
            if resource['name'] == resource_name:
                return True

        return False

    def create_resource(self, packageid, resource_name):
        """
        Creates new resource in ckan instance
        :param packageid: dataset under which to add new resource
        :param resource_name: name of new resource
        :return: id of newly created resource if successful
        """

        # Make api call
        create_resource = requests.post(
            self.ckan_url + 'action/resource_create',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'package_id': packageid,
                'url': '#',
                'name': resource_name,
                'url_type': 'datapusher',
                'format': 'CSV'
            })
        )

        resource = json.loads(create_resource.text)

        if not resource['success']:
            print(resource)
            print("ERROR: {}".format(resource['error']['name'][0]))
            return

        print("SUCCESS: Resource # {} was created.".format(resource['result']['id']))
        return resource['result']['id']

    def create_datastore(self, resource, fields, keys=None):
        """
        Creates new datastore for specified resource
        :param resource: resource id fo which new datastore is being made
        :param fields: header fields for csv file
        :return: resource id if successful
        """

        # Make API call
        datastore_creation = requests.post(
            self.ckan_url + 'action/datastore_create',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource,
                'primary_key': keys,
                'force': True,
                'fields': fields
            })
        )

        datastore = json.loads(datastore_creation.text)

        if not datastore['success']:
            print("ERROR: {}".format(datastore['error']['name'][0]))
            return
        print("SUCCESS: Datastore # {} was created.".format(datastore['result']['resource_id']))
        return datastore['result']

    def delete_datastore(self, resource):
        """
        Deletes datastore table for resource
        :param resource: resource to remove table from
        :return: request status
        """
        delete = requests.post(
            self.ckan_url + 'action/datastore_delete',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource,
                'force': True
            })
        )
        return delete.status_code

    def upsert(self, resource, data, method='insert'):
        """
        Upsert data into datastore
        :param resource: resource to which data will be inserted
        :param data: data to be upserted
        :return: request status
        """
        insert = requests.post(
            self.ckan_url + 'action/datastore_upsert',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource,
                'method': method,
                'force': True,
                'records': data
            })
        )
        return insert

    def update_meta_data(self, resource):
        """
        TODO: Make this versatile
        :param resource: resource whose metadata willbe modified
        :return: request status
        """
        update = requests.post(
            self.ckan_url + 'action/resource_patch',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'id': resource,
                'url': self.dump_url + resource,
                'url_type': 'datapusher',
                'last_modified': datetime.now().isoformat(),
            })
        )
        return update.status_code

    def resource_search(self, name):
        """

        :param name:
        :return:
        """
        search = requests.post(
            self.ckan_url + 'action/datastore_search',
            headers={
                'content-type': 'application/json',
                'authorization':self.key
            },
            data=json.dumps({
                "resource_id": name,
                "plain": False
            })
        )

        return search
