# Use Python 3.9 slim as the base image
FROM python:3.9-slim-buster

########################################################
# Install essential Linux packages
########################################################
RUN apt-get update && apt-get install -y \
        zip \
        g++ \
        gcc \
        make \
        cmake \
        autoconf \
        automake \
        unzip \
        perl \
        git \
        wget \
        libssl-dev \
        libncurses5-dev \
        zlib1g-dev \
        libxslt-dev \
        libxml2-dev \
        zlib1g-dev \
        liblzma-dev \
        libbz2-dev \
        gawk \
        && rm -rf /var/lib/apt/lists/* \
        && apt-cache search linux-headers-generic

# Update package lists (again, in case of changes)
RUN apt-get update

########################################################
# Install Python modules needed for serverless execution
########################################################
RUN pip install --upgrade --ignore-installed pip wheel six setuptools \
    && pip install --upgrade --no-cache-dir --ignore-installed \
        awslambdaric \
        boto3 \ 
        bottleneck \ 
        redis \
        httplib2 \
        requests \
        numpy \
        scipy \
        pandas \
        pika \
        cloudpickle \
        ps-mem \
        tblib \
        s3fs \
        ujson \
        h5py \
        fsspec \
        xarray \
        zarr \
        cftime \
        dask

# Copy Lithops Lambda runtime package into the function directory
COPY lithops_lambda.zip ${FUNCTION_DIR}
RUN unzip lithops_lambda.zip \
    && rm lithops_lambda.zip \
    && mkdir handler \
    && touch handler/__init__.py \
    && mv entry_point.py handler/

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD [ "handler.entry_point.lambda_handler" ]