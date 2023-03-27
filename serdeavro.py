import copy
import json
import avro
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader

# Note that we combined namespace and name to get "full name"
schema = {
    'name': 'avro.example.User',
    'type': 'record',
    'fields': [
        {'name': 'name', 'type': 'string'},
        {'name': 'age', 'type': 'int'}
    ]
}

# Parse the schema so we can use it to write the data
schema_parsed = avro.schema.Parse(json.dumps(schema))

# Write data to an avro file
with open('users.avro', 'wb') as f:
    writer = DataFileWriter(f, DatumWriter(), schema_parsed)
    writer.append({'name': 'Pierre-Simon Laplace', 'age': 77})
    writer.append({'name': 'John von Neumann', 'age': 53})
    writer.close()

# Read data from an avro file
with open('users.avro', 'rb') as f:
    reader = DataFileReader(f, DatumReader())
    metadata = copy.deepcopy(reader.meta)
    schema_from_file = json.loads(metadata['avro.schema'])
    users = [user for user in reader]
    reader.close()

print(f'Schema that we specified:\n {schema}')
print(f'Schema that we parsed:\n {schema_parsed}')
print(f'Schema from users.avro file:\n {schema_from_file}')
print(f'Users:\n {users}')