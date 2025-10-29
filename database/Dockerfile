# Usamos la imagen oficial de PostgreSQL
FROM postgres:latest

# Copiamos los scripts de inicialización dentro del contenedor
COPY init.sql /docker-entrypoint-initdb.d/
COPY test.csv /docker-entrypoint-initdb.d/
COPY load_data.sql /docker-entrypoint-initdb.d/

