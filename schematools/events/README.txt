Eventprocessor that does crud operations on json blob objects.

Spark has it own internal kv store. GOB data will be stored
in this kv storeas json blobs, addressed by a unique key with the following
components:  dataset - table - identificatie [ - volgnummer]

Course of action is as follows:

- GOB Events comes in (ADD/MODIFY/DELETE) from Kafka
- For ADD:
  - Create json blob for the record
  - Store json blob in spark kv store
  - Create Kafka event with this record
- FOR MODIFY:
  - Fetch json blob from spark kv store (based on unique key, see above)
  - Update json blob with the incoming partial event data
  - Store json blob in spark kv store
  - Create Kafka event with the complete record (from the json blob)
- For DELETE:
  - Delete json blob in spark kv store (base on unique key)
  - Create Kafka delete event for this record


The current implementation updates the relational database with
the incoming event data from GOB. This implementation should be converted
so that it modifies the json blob.

Open questions:

How do we handle relations?
Relations in kv store: left-ds - table - left-ident - right-ds - table - right-ident


