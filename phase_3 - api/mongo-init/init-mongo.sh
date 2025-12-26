#!/bin/bash
echo "--- Starting MongoDB Restore from directory ---"

# The mongorestore command is designed to work with a directory.
# We point it to the 'FaithInterFolia' directory that is now inside /docker-entrypoint-initdb.d/
# It will automatically find all the .bson and .metadata.json files inside.
mongorestore --db FaithInterFolia /docker-entrypoint-initdb.d/FaithInterFolia

echo "--- MongoDB Restore Finished ---"