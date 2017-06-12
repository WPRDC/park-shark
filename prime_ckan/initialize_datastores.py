import json
from datapusher import Datapusher
from remote_parameters import resource_id, ad_hoc_resource_id
from gadgets import query_yes_no, fire_grappling_hook, get_resource_name, get_package_name_from_resource_id, get_number_of_rows

server="Live"
site, API_key, settings = fire_grappling_hook('ckan_settings.json',server)

resource_name = get_resource_name(site,resource_id,API_key)
ad_hoc_resource_name = get_resource_name(site,ad_hoc_resource_id,API_key)
package_name = get_package_name_from_resource_id(site,resource_id,API_key)
rows = get_number_of_rows(site,resource_id,API_key)

question = "Do you really want to initalize (and possibly delete) these datastores? "
question += "({} ({} with {} rows) and {} ({}), both from {})"
question = question.format(resource_name,resource_id,rows,ad_hoc_resource_name,ad_hoc_resource_id, package_name)

response = query_yes_no(question, "no")


if response:
    dp = Datapusher(settings, server=server)
    dp.delete_datastore(resource_id)
    ordered_fields = [{"id": "Zone", "type": "text"}]
    ordered_fields.append({"id": "Start", "type": "timestamp"})
    ordered_fields.append({"id": "End", "type": "timestamp"})
    ordered_fields.append({"id": "UTC Start", "type": "timestamp"})
    ordered_fields.append({"id": "Transactions", "type": "int"})
    ordered_fields.append({"id": "Car-minutes", "type": "int"})
    ordered_fields.append({"id": "Payments", "type": "float"})
    ordered_fields.append({"id": "Durations", "type": "json"})

    keys = ["Zone", "UTC Start"]
    call_result = dp.create_datastore(resource_id, ordered_fields, keys=keys)
    print("Datastore creation result: {}".format(call_result))
    ###############################################################

    dp = Datapusher(settings, server=server)
    dp.delete_datastore(ad_hoc_resource_id)
    ordered_fields = [{"id": "Zone", "type": "text"}]
    ordered_fields.append({"id": "Parent Zone", "type": "text"})
    ordered_fields.append({"id": "Start", "type": "timestamp"})
    ordered_fields.append({"id": "End", "type": "timestamp"})
    ordered_fields.append({"id": "UTC Start", "type": "timestamp"})
    ordered_fields.append({"id": "Transactions", "type": "int"})
    ordered_fields.append({"id": "Car-minutes", "type": "int"})
    ordered_fields.append({"id": "Payments", "type": "float"})
    ordered_fields.append({"id": "Durations", "type": "json"})


    keys = ["Zone", "UTC Start"]
    call_result = dp.create_datastore(ad_hoc_resource_id, ordered_fields, keys=keys)
    print("Datastore creation result: {}".format(call_result))
else:
    print("OK, no changes were made.")
