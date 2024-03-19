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

RUN cd /duckdb_iceberg/duckdb/tools/pythonpkg && python3 -m pip wheel .

FROM rubiklabs/base:ubuntu23

RUN mkdir -p /duckdb/repository

COPY --from=builder /duckdb_iceberg/duckdb/tools/pythonpkg/duckdb-0.10.0-cp311-cp311-linux_x86_64.whl /duckdb/duckdb-0.10.0-cp311-cp311-linux_x86_64.whl
COPY --from=builder /duckdb_iceberg/build/release/repository /duckdb/repository
COPY --from=builder /duckdb_iceberg/build/release/duckdb /duckdb/

WORKDIR /duckdb