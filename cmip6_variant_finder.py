# CMIP6 Model Variant Finder
# 
# This script identifies CMIP6 model variants with complete data for a specified variable
# across multiple scenarios and time resolutions for climate analysis.

import xarray as xr
import s3fs
import pandas as pd
import numpy as np
from functools import reduce

def setup_dask_cluster():
    """Set up and return a Dask distributed client for parallel processing."""
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster()
    return Client(cluster)

def find_model_variants(variable='tasmax', time_resolution='Amon', scenarios=None):
    """
    Find CMIP6 models that have data for all specified scenarios and the given time resolution.
    
    Args:
        variable (str): Variable to search for (e.g., 'tasmax')
        time_resolution (str): Time resolution (e.g., 'Amon' for monthly, 'day' for daily)
        scenarios (dict): Dictionary of scenario names and descriptions
        
    Returns:
        tuple: (DataFrame of scenario runs, DataFrame of historical runs)
    """
    s3 = s3fs.S3FileSystem(anon=False)
    
    # Find all available model runs for the specified variable and time resolution
    def get_model_runs(experiment, variable, time_resolution):
        fns = s3.glob(f's3://cmip6-pds/CMIP6/*/*/{experiment}/*/{time_resolution}/{variable}/')
        if not fns:
            return None
        models = [fn.split('/')[4] for fn in fns]  # Model names
        variant = [fn.split('/')[6] for fn in fns]  # Variant IDs
        return pd.DataFrame({
            'model': models,
            'variant': variant,
            'url': fns
        }).set_index(['model', 'variant'])

    # Get runs for each scenario
    scenario_runs = []
    for scenario in scenarios:
        runs = get_model_runs(scenario, variable, time_resolution)
        if runs is not None:
            runs['scenario'] = scenario
            scenario_runs.append(runs)
    
    scenario_runs = pd.concat(scenario_runs) if scenario_runs else pd.DataFrame()
    
    # Get historical runs
    historical_runs = get_model_runs('historical', variable, time_resolution) or pd.DataFrame()
    
    return scenario_runs, historical_runs

def find_common_models(scenario_runs, historical_runs, scenarios):
    """
    Find model variants that have data for all specified scenarios and historical runs.
    """
    if scenario_runs.empty or historical_runs.empty:
        return pd.Index([])
        
    common_indices = reduce(
        np.intersect1d,
        [scenario_runs[scenario_runs.scenario == scen].index.values for scen in scenarios] +
        [historical_runs.index.values]
    )
    return common_indices

def main(variable='tasmax', time_resolution='Amon'):
    """
    Main function to find and process CMIP6 model variants.
    
    Args:
        variable (str): Variable to search for (e.g., 'tasmax')
        time_resolution (str): Time resolution (e.g., 'Amon' for monthly, 'day' for daily)
    """
    # Define scenarios of interest
    SCENARIOS = {
        'ssp245': 'SSP2-4.5 - Middle of the road scenario',
        'ssp585': 'SSP5-8.5 - High emissions scenario',
        'ssp126': 'SSP1-2.6 - Low emissions scenario',
        'ssp370': 'SSP3-7.0 - Medium-high emissions scenario'
    }
    
    print(f"Searching for {variable} data with {time_resolution} time resolution...")
    
    # Find model variants
    scenario_runs, historical_runs = find_model_variants(
        variable=variable,
        time_resolution=time_resolution,
        scenarios=SCENARIOS
    )
    
    # Find common model variants
    common_indices = find_common_models(scenario_runs, historical_runs, SCENARIOS)
    
    if len(common_indices) == 0:
        print("No common model variants found for the specified criteria.")
        return
    
    # Create output filename based on variable and time resolution
    output_file = f"cmip6_{variable}_{time_resolution}_urls.csv"
    
    # Save results
    result = pd.DataFrame({
        'model': [idx[0] for idx in common_indices],
        'variant': [idx[1] for idx in common_indices]
    })
    
    # Add scenario URLs
    for scenario in SCENARIOS:
        mask = (scenario_runs.index.isin(common_indices)) & (scenario_runs['scenario'] == scenario)
        result[scenario] = scenario_runs[mask]['url'].values
    
    result['historical'] = historical_runs.loc[common_indices, 'url'].values
    
    result.to_csv(output_file, index=False)
    print(f"Found {len(common_indices)} common model variants.")
    print(f"Results saved to {output_file}")
    
    return result

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Find CMIP6 model variants with complete tasmax data.')
    parser.add_argument('--variable', type=str, default='tasmax',
                       help='Variable to search for (e.g., tasmax, tas, pr)')
    parser.add_argument('--time_resolution', type=str, default='Amon',
                       help='Time resolution (e.g., Amon for monthly, day for daily)')
    
    args = parser.parse_args()
    
    # Set up Dask client
    client = setup_dask_cluster()
    
    try:
        main(variable=args.variable, time_resolution=args.time_resolution)
    finally:
        # Clean up Dask client
        client.close()
        client.cluster.close()
