FROM docker.io/python:3.11.1 as builder

# Install basic dependencies
RUN apt-get update && apt-get install -y \
    git curl build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev wget llvm \
    make cmake

# Create vcpkg directory
RUN mkdir -p /usr/local/vcpkg

# Download and install vcpkg
WORKDIR /usr/local/vcpkg

RUN git clone https://github.com/microsoft/vcpkg .

RUN apt-get install -y zip unzip tar && chmod +x bootstrap-vcpkg.sh && ./bootstrap-vcpkg.sh

# Add vcpkg to PATH
ENV PATH="$PATH:/usr/local/vcpkg/scripts"

# # Clone duckdb_iceberg repository
RUN mkdir /duckdb_iceberg && cd /duckdb_iceberg && wget https://github.com/devendrasr/duckdb_iceberg/archive/master.zip && unzip master.zip && mv duckdb_iceberg-master/* . && rm -rf master.zip

RUN cd /duckdb_iceberg && rm -rf duckdb_iceberg-master && rm -rf duckdb && git clone https://github.com/duckdb/duckdb.git && cd duckdb && git checkout v0.10.0

## Build duckdb_iceberg with vcpkg
RUN cd /duckdb_iceberg \
    && /usr/local/vcpkg/vcpkg install

RUN cd /duckdb_iceberg && VCPKG_TOOLCHAIN_PATH=/usr/local/vcpkg/scripts/buildsystems/vcpkg.cmake make release

FROM rubiklabs/base:ubuntu23

RUN mkdir /duckdb/

COPY --from=builder /duckdb_iceberg/build/release/repository /duckdb/repository
COPY --from=builder /duckdb_iceberg/build/release/duckdb /duckdb/

# RUN mkdir /duckdb/ \
#     && mv /duckdb_iceberg/build/release/repository /duckdb/repository \
#     && mv /duckdb_iceberg/build/release/duckdb /duckdb/ \
#     && rm -rf /duckdb_iceberg && rm -rf /usr/local/vcpkg \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*


WORKDIR /duckdb

# FROM node:18.13.0-bullseye-slim

# FROM node:18.13.0-bullseye-slim

# COPY --from=builder /duckdb_iceberg/build/release/repository /repository

# COPY --from=builder /duckdb_iceberg /duckdb_iceberg

# WORKDIR /duckdb-nodejs

# RUN rm -rf /duckdb_iceberg \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*

