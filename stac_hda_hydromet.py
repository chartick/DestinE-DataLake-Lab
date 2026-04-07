import os
from datetime import datetime
from shapely.geometry import box
import pystac
import xarray as xr
import pandas as pd
import re
import yaml
import shutil
import json


def extract_datetime_from_filename(filename):
    """Extract datetime from a NetCDF or csv filename."""
    match = re.search(
        r"(\d{4})_\d{2}_\d{2}_T\d{2}_\d{2}_to_"
        r"(\d{4})_\d{2}_\d{2}_T\d{2}_\d{2}",
        filename
    )
    start_year = int(match.group(1))
    end_year = int(match.group(2))

    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    return start_date, end_date


def get_spatial_extent_csv(dataset):
    """Extract spatial extent (bounding box) from a csv dataset."""
    lat_min, lat_max = dataset.lat_center.min().item(
        ), dataset.lat_center.max().item()
    lon_min, lon_max = dataset.lon_center.min().item(
        ), dataset.lon_center.max().item()
    return [lon_min, lat_min, lon_max, lat_max]


def get_spatial_extent(dataset):
    """Extract spatial extent (bounding box) from a NetCDF dataset."""
    lat_min, lat_max = dataset.lat.min().item(), dataset.lat.max().item()
    lon_min, lon_max = dataset.lon.min().item(), dataset.lon.max().item()
    return [lon_min, lat_min, lon_max, lat_max]


def create_stac_collection(output_path, spatial_extent,
                           temporal_extent, eumet_id, title, description,
                           short_description,
                           parameters_path="parameters.json"):
    """Create a STAC Collection and save it."""
    print(spatial_extent, temporal_extent)

    # Load parameters.json
    with open(parameters_path, "r") as f:
        parameters_data = json.load(f)

    asset = pystac.Asset(
        href="https://destination-earth.eu/wp-content/uploads/2023/10/hydromet_fig1-1771x547-1.png",
        media_type="image/png",
        roles=["thumbnail"],
        title="overview"
    )

    collection = pystac.Collection(
        id=eumet_id,
        description=description,
        stac_extensions=["https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
                         "https://stac-extensions.github.io/timestamps/v1.1.0/schema.json",
                         "https://stac-extensions.github.io/datacube/v2.2.0/schema.json"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([spatial_extent]),
            temporal=pystac.TemporalExtent([temporal_extent]),
        ),
        license="CC-BY-4.0",
        title=title,
        summaries={"federation:backends": ["dedt_lumi"]},
        keywords=["NetCDF", "Climate", "Precipitation", "Extreme events"],
        providers=[
            pystac.Provider(
                name="Deutscher Wetterdienst (DWD)",
                roles=["producer"],
                url="https://www.dwd.de/",
            ),
            pystac.Provider(
                name="European Centre for Medium-Range Weather Forecasts (ECMWF)",
                roles=["licensor"],
                url="https://www.ecmwf.int/",
            ),
            pystac.Provider(
                name="Destination Earth Data Lake (DEDL)",
                roles=["host"],
                url="https://data.destination-earth.eu/",
            )
        ],
        extra_fields={
            "dedl:short_description": short_description,
            "sci:publications": [
            {
            "doi": "10.1127/metz/2021/1088",
            "citation": "Lengfeld, K., Walawender, E., Winterrath, T., & Becker, A. (2021). CatRaRE: A Catalogue of radar-based heavy rainfall events in Germany derived from 20 years of data. Meteorologische Zeitschrift, 30(6), 469–487. https://doi.org/10.1127/metz/2021/1088"
            },
            {
            "doi": "10.5194/hess-27-1109-2023",
            "citation": "Shehu, B., Willems, W., Stockel, H., Thiele, L.-B., & Haberlandt, U. (2023). Regionalisation of rainfall depth–duration–frequency curves with different data types in Germany. Hydrology and Earth System Sciences, 27(5), 1109–1132. https://doi.org/10.5194/hess-27-1109-2023"
            }
            ],
            "cube:dimensions": parameters_data["cube:dimensions"],
            "cube:variables": parameters_data["cube:variables"]
        }
    )

    collection.add_link(
    pystac.Link(
        rel="related",
        target="https://destination-earth.eu/use-cases/simulating-the-future-of-extreme-events/",
        media_type="text/html",
        title="Simulating the Future of Extreme Events"
    )
)

    collection.add_asset("thumbnail", asset)
    collection.save_object(dest_href=output_path)

    print(f"Collection saved to {output_path}")


def create_stac_item_nc(netcdf_path, output_path):
    """Create a STAC Item for a given NetCDF file and save it."""
    dataset = xr.open_dataset(netcdf_path)

    # Extract metadata
    filename = os.path.basename(netcdf_path)
    datetime_obj_start, datetime_obj_end = extract_datetime_from_filename(
        filename)
    spatial_extent = get_spatial_extent(dataset)
    geometry = box(*spatial_extent).__geo_interface__

    # Create STAC Item
    item = pystac.Item(
        id=filename.replace("." + "nc", ""),
        geometry=geometry,
        bbox=spatial_extent,
        datetime=None,
        start_datetime=datetime_obj_start,
        end_datetime=datetime_obj_end,
        properties={},
    )

    # Add asset
    item.add_asset(
        key="data",
        asset=pystac.Asset(
            href="",
            media_type="application/x-netcdf",
            title="NetCDF File",
            roles=["data"],
            ),
        )

    # Save STAC Item
    item.save_object(dest_href=output_path)
    print(f"STAC Item saved to {output_path}")


def create_stac_item_csv(netcdf_path, output_path):
    """Create a STAC Item for a given NetCDF file and save it."""
    dataset = pd.read_csv(netcdf_path, delimiter=';', comment='#')
    dataset['lat_center'] = pd.to_numeric(
        dataset['lat_center'], errors='coerce')
    dataset['lon_center'] = pd.to_numeric(
        dataset['lon_center'], errors='coerce')

    # Extract metadata
    filename = os.path.basename(netcdf_path)
    datetime_obj_start, datetime_obj_end = extract_datetime_from_filename(
        filename)
    spatial_extent = get_spatial_extent_csv(dataset)
    geometry = box(*spatial_extent).__geo_interface__

    # Create STAC Item
    item = pystac.Item(
        id=filename.replace("." + "csv", ""),
        geometry=geometry,
        bbox=spatial_extent,
        datetime=None,
        start_datetime=datetime_obj_start,
        end_datetime=datetime_obj_end,
        properties={},
    )

    # Add asset
    item.add_asset(
        key="data",
        asset=pystac.Asset(
            href="",
            media_type="text/plain",
            title="csv File",
            roles=["data"],
            ),
        )

    # Save STAC Item
    item.save_object(dest_href=output_path)
    print(f"STAC Item saved to {output_path}")


def create_item_config(data_dir, file, filetype):
    "Create item_config.json in the item folder of the data directory"
    if filetype == "nc":
        dataset = xr.open_dataset(file)
        spatial_extent = get_spatial_extent(dataset)
    else:
        dataset = pd.read_csv(netcdf_path, delimiter=';', comment='#')
        dataset['lat_center'] = pd.to_numeric(
            dataset['lat_center'], errors='coerce')
        dataset['lon_center'] = pd.to_numeric(
            dataset['lon_center'], errors='coerce')
        spatial_extent = get_spatial_extent_csv(dataset)

    config = {"bbox": spatial_extent}

    # Write to collection_config.json
    with open(data_dir + "/item_config.json", "w") as file:
        json.dump(config, file, indent=4)


def create_config_json(eumet_id, metadata_dir):
    """Creates the top level collection_config.json """

    config = {
        "id": eumet_id,
        "item_asset_ignore_list": ["item_config.json"],
        "item_folder_level": "YYYY",
        "thumbnail_regex": "^thumbnail",
        "overview_regex": "^overview",
        "additional_property_keys": ["model", "experiment",
                                     "generation", "realization",
                                     "activity", "resolution"]
    }

    # Write to collection_config.json
    with open(metadata_dir + "/collection_config.json", "w") as file:
        json.dump(config, file, indent=4)


if __name__ == "__main__":
    
    pystac.version.set_stac_version("1.0.0")

    # Get extended attributes from file
    with open("catalog_config.yaml", "r") as file:
        config = yaml.safe_load(file)
    title = config.get("title", "No title found")
    description = config.get("description", "No description found")
    eumet_id = config.get("id", "No id found")
    generation = config.get("generation", "No generation found")
    short_description = config.get(
        "short_description", "No short description found")

    # Directory structure
    base_dir = os.getcwd()
    org_dir = os.path.join(base_dir, "appdata")
    coll_dir = os.path.join(base_dir, eumet_id)
    metadata_dir = os.path.join(coll_dir, "metadata")
    items_dir = os.path.join(metadata_dir, "items")
    data_dir = os.path.join(coll_dir, "data")
    pdf_path = os.path.join(org_dir, "hydromet_output.pdf")

    # Ensure directories exist
    os.makedirs(coll_dir, exist_ok=True)
    os.makedirs(metadata_dir, exist_ok=True)
    os.makedirs(items_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)

    # Initialize spatial and temporal extents
    all_bboxes = []
    all_datetimes = []

    # Process each NetCDF file
    for model in os.listdir(org_dir):
        model_path = os.path.join(org_dir, model)
        if os.path.isdir(model_path):
            for file in os.listdir(model_path):
                if file.endswith(".nc"):
                    netcdf_path = os.path.join(model_path, file)
                    item_path = os.path.join(items_dir,
                                             f"{file.replace('.nc', '.json')}")

                    # Create STAC Item
                    dataset = xr.open_dataset(netcdf_path)
                    bbox = get_spatial_extent(dataset)
                    all_bboxes.append(bbox)

                    datetime_obj_start, datetime_obj_end =\
                        extract_datetime_from_filename(file)
                    if 1990 <= datetime_obj_start.year <= 2019:
                        activity = "baseline"
                        experiment = "hist"
                        exp_start = "1990"
                        exp_end = "2014"
                        resolution = "5km"
                        realization = "1"
                    else:
                        activity = "projections"
                        experiment = "ssp3-7.0"
                        exp_start = "2015"
                        exp_end = "2049"
                        resolution = "5km"
                        realization = "1"
                    year_dir = os.path.join(data_dir, exp_start)
                    os.makedirs(year_dir, exist_ok=True)
                    file_struct = os.path.join(year_dir, eumet_id + "_" +
                                               exp_start + "0101T000000_" +
                                               exp_end + "1231T230000__" +
                                               model +
                                               "__" +
                                               experiment +
                                               "__" +
                                               str(generation) +
                                               "__" +
                                               str(realization) +
                                               "__" +
                                               str(activity) +
                                               "__" +
                                               str(resolution))
                    os.makedirs(file_struct, exist_ok=True)
                    shutil.copy(netcdf_path, file_struct)
                    shutil.copy(pdf_path, file_struct)
                    create_item_config(file_struct, netcdf_path, "nc")
                    all_datetimes.append(datetime_obj_start)
                    all_datetimes.append(datetime_obj_end)

                    # create_stac_item_nc(netcdf_path, item_path)

                if file.endswith(".csv"):
                    netcdf_path = os.path.join(model_path, file)
                    item_path = os.path.join(
                        items_dir, f"{file.replace('.csv', '.json')}")

                    # Create STAC Item
                    dataset = pd.read_csv(
                        netcdf_path, delimiter=';', comment='#')
                    dataset['lat_center'] = pd.to_numeric(
                        dataset['lat_center'], errors='coerce')
                    dataset['lon_center'] = pd.to_numeric(
                        dataset['lon_center'], errors='coerce')
                    bbox = get_spatial_extent_csv(dataset)
                    all_bboxes.append(bbox)

                    datetime_obj_start, datetime_obj_end = \
                        extract_datetime_from_filename(file)
                    if 1990 <= datetime_obj_start.year <= 2014:
                        activity = "baseline"
                        experiment = "hist"
                        exp_start = "1990"
                        exp_end = "2014"
                        resolution = "5km"
                        realization = "1"
                    else:
                        activity = "projections"
                        experiment = "ssp3-7.0"
                        exp_start = "2015"
                        exp_end = "2049"
                        resolution = "5km"
                        realization = "1"
                    year_dir = os.path.join(data_dir, exp_start)
                    os.makedirs(year_dir, exist_ok=True)
                    file_struct = os.path.join(year_dir, eumet_id + "_" +
                                               exp_start + "0101T000000_" +
                                               exp_end + "1231T230000__" +
                                               model +
                                               "__" +
                                               experiment +
                                               "__" +
                                               str(generation) +
                                               "__" +
                                               str(realization) +
                                               "__" +
                                               str(activity) +
                                               "__" +
                                               str(resolution))
                    os.makedirs(file_struct, exist_ok=True)
                    shutil.copy(netcdf_path, file_struct)
                    shutil.copy(pdf_path, file_struct)
                    if not os.path.exists(file_struct + "item_config.json"):
                        create_item_config(file_struct, netcdf_path, "csv")
                    all_datetimes.append(datetime_obj_start)
                    all_datetimes.append(datetime_obj_end)

                    # create_stac_item_csv(netcdf_path, item_path)

    # Determine collection extents
    spatial_extent = [
        min([b[0] for b in all_bboxes]),
        min([b[1] for b in all_bboxes]),
        max([b[2] for b in all_bboxes]),
        max([b[3] for b in all_bboxes]),
    ]
    temporal_extent = [
        datetime(1990, 1, 1),
        datetime(2049, 12, 31),
    ]
    # Create STAC Collection
    collection_path = os.path.join(metadata_dir, "collection.json")
    create_stac_collection(collection_path, spatial_extent,
                           temporal_extent, eumet_id, title, description,
                           short_description)
    create_config_json(eumet_id, metadata_dir)
