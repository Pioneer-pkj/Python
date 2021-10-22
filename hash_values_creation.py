from fastavro import reader
import  hashlib
filename = 'data/UM_CUST_TEST.20200926.A901.avro'
hash_filename = 'data/hash_file.avro.hash'

with open(filename, 'rb') as source_file:
    with open(hash_filename, 'wb') as hash_file:
        avro_reader = reader(source_file)
        for line in avro_reader:
            print(hashlib.md5('|'.join(line.values()).encode('utf-8')).digest())