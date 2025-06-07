###################################
#                                 # 
#        Serverless Demo          #
#                                 #
###################################

import lithops
import awswrangler as wr
import xarray as xr
import numpy as np
import argparse
import concurrent.futures
import pandas as pd
import datetime
import s3fs
import logging

std_attrs = {
    'title':'Serverless Climate Data Processing Demo',
    'institution':'NC Institute for Climate Studies',
    'date': datetime.datetime.now().isoformat()
}

def parse_arguments():
    """
    Parses command-line arguments for the script.
    Returns:
        argparse.Namespace: Parsed arguments with memory per worker, image name, and output bucket.
    """
    import os
    parser = argparse.ArgumentParser(description="Serverless Demo")
    parser.add_argument("--mem", type=int, default=3000, help="Memory per lithops worker in MB")
    parser.add_argument(
        "--image-name",
        type=str,
        default=os.environ.get("IMAGE_NAME", "serverless-demo:latest"),
        help="Lithops runtime Docker image name (default: env IMAGE_NAME or 'serverless-demo:latest')"
    )
    parser.add_argument(
        "--output-bucket",
        type=str,
        default=os.environ.get("OUTPUT_BUCKET"),
        required=os.environ.get("OUTPUT_BUCKET") is None,
        help="Output S3 bucket for results (required if OUTPUT_BUCKET env var is not set)"
    )
    return parser.parse_args()
    
def get_attrs(url_in):
    """
    Extracts model, scenario, and variant attributes from the input URL and merges with standard attributes.
    Args:
        url_in (str): Input Zarr dataset URL.
    Returns:
        dict: Attributes dictionary.
    """
    attrs = std_attrs.copy()
    attrs['model'] = url_in.split('/')[6] 
    attrs['scenario'] =  url_in.split('/')[7] 
    attrs['variant'] = url_in.split('/')[8] 
    return attrs
    
def preprocess(func, url_in, url_out):
    """
    Preprocesses the input dataset and applies the provided function.
    Args:
        func (callable): Function to apply to the dataset.
        url_in (str): Input Zarr dataset URL.
        url_out (str): Output Zarr dataset URL.
    Returns:
        tuple: (processed xarray.Dataset, output URL)
    """
    ds = xr.open_zarr(url_in).hus
    logger.info(f"Loaded " + url_in + ".")
    ds = ds.convert_calendar('366_day', missing=np.nan)
    ds = ds.chunk({'time': 366})
    
    result = func(ds)
    attrs = {**result.attrs, **get_attrs(url_in)}
    result.attrs = attrs
    
    result.to_zarr(url_out, compute=False, mode='w')
    
    return ds, url_out

def process(func, url_in, url_out, yr):
    """
    Processes the dataset for a specific year by applying the provided function.
    Args:
        func (callable): Function to apply to the dataset.
        url_in (str): Input Zarr dataset URL.
        url_out (str): Output Zarr dataset URL.
        yr (int): Year to filter the results.
    Returns:
        None
    """
    ds = xr.open_zarr(url_in).hus
    ds = ds.convert_calendar('366_day', missing=np.nan)
    ds = ds.chunk({'time': 366})
    result = func(ds)
    result = result.where(result.time.dt.year==yr, drop=True)
    attrs = {**result.attrs, **get_attrs(url_in)}
    result.attrs = attrs

    result.to_zarr(url_out, region='auto')
        
def call_lambda(fexec, func, url_in, url_out):
    #lazy load the entire dataset in order to retrieve length of time dimension for function mapping below and initialize empty zarr store
    ds, url_out = preprocess(func, url_in, url_out)
    
    # Run the process over the objects
    args = [(func, url_in, url_out, yr) for yr in np.unique(ds.time.dt.year)]
    
    fexec.map(process, args)

    # out = xr.open_zarr(url_out, decode_times=False).fillna(0)
    # if np.isfinite(out[list(out.data_vars)[0]]).mean('time').sum().values < 311464:
    #     return 'ERROR: ' + url_out + ' did not process correctly'
        
    return url_in

def tpw(ds):
    """
    Calculate total precipitable water (TPW) from a humidity dataset.
    TPW is computed by vertically integrating the humidity variable over pressure levels.

    This is an example calculation function. You can duplicate and adapt this function for any custom variable or calculation you wish to perform on your climate data.

    Args:
        ds (xarray.DataArray): Input humidity dataset (with 'plev' dimension).
    Returns:
        xarray.Dataset: Dataset containing the TPW variable in millimeters (mm).
    """
    ds.attrs = {}
    ds = -ds.fillna(0).integrate('plev').to_dataset(name='tpw')/9.807
    ds.attrs['units'] = "mm"
    
    return ds

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler()]
    )
    logger = logging.getLogger(__name__)

    logger.info("Script started.")
    args = parse_arguments()
    mem = args.mem
    image_name = args.image_name
    output_bucket = args.output_bucket

    s3 = s3fs.S3FileSystem(anon=False)
    try:
        urls_in = pd.read_csv('./urls.csv').urls
        logger.info(f"Loaded {len(urls_in)} URLs from urls.csv.")
        urls_in = ['s3://' + s3.glob('s3://' + url + '/*/*/')[-1] for url in urls_in]
        logger.info(f"Resolved S3 input paths for all URLs.")
    except Exception as e:
        logger.error(f"Error loading or resolving URLs: {e}")
        raise

    # Configure the Lithops (Lambda) Backend
    logger.info("Configuring Lithops FunctionExecutor.")
    fexec = lithops.FunctionExecutor(
            log_level='DEBUG',
            runtime = image_name,
            runtime_memory = mem,
            runtime_timeout = 300
    ) 
    
    urls_out=[f's3://{output_bucket}/' + url.split('/')[3] + '_' + url.split('/')[4] + '_' + url.split('/')[5] + '_' + url.split('/')[6] + '_' + url.split('/')[7] + '_' + url.split('/')[8] + '_tpw' for url in urls_in]
    logger.info(f"Prepared {len(urls_out)} output S3 URLs (bucket: {output_bucket}).")

    args=[(fexec, tpw, urls_in[urli], urls_out[urli]) for urli in range(0,len(urls_in))]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        logger.info("Submitting jobs to the thread pool executor.")
        # Submit all the tasks and wait for them to complete

        futures = [executor.submit(call_lambda, *arg) for arg in args]
        logger.info(f"Submitted {len(futures)} files for processing to AWS via Lithops.")
        
        # Wait for all futures to complete, this is optional if you just want to fire and forget
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                result = future.result()
                logger.info(f"Completed processing for URL {i+1}/{len(futures)}: {result}")
            except Exception as e:
                logger.error(f"Error processing URL {i+1}/{len(futures)}: {e}")


 
