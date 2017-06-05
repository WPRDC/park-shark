import json
from datapusher import Datapusher
from remote_parameters import resource_id, ad_hoc_resource_id

print("Insert a 'Do you really want to do this?' prompt here.")

#Practice Parking|Correctly Typed Parking Data
#resource_id = "b4387271-82a3-4d87-bc82-e4fc033ecc50"
with open('ckan_settings.json') as f:
    settings = json.load(f)

server="Live"
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
#Practice Parking|Correctly Typed Parking Data by Ad Hoc Zone
#ad_hoc_resource_id = "62e825e2-4597-43fe-845e-434eecfd5aba"

with open('ckan_settings.json') as f:
    settings = json.load(f)

server="Live"
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