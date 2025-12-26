# database/Dockerfile
FROM mongo:4.4

# We removed "../" because we build from the main folder, 
# so "mongo-init" is right there in the context.
COPY mongo-init/ /docker-entrypoint-initdb.d/