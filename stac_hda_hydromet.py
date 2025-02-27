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


def extract_datetime_from_filename(filename, ftype):
    """Extract datetime from a NetCDF or csv filename."""
    if ftype == "nc":
        match = re.search(r'(\d{4})_(\d{4})', filename)
        start_year = int(match.group(1))
        end_year = int(match.group(2))

        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)
        return start_date, end_date
    if ftype == "csv":
        match = re.search(r'(\d{10})-(\d{10})', filename)
        start_date_str = match.group(1)
        end_date_str = match.group(2)

        start_date = datetime.strptime(start_date_str, '%Y%m%d%H')
        end_date = datetime.strptime(end_date_str, '%Y%m%d%H')
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
                           temporal_extent,  eumet_id, title, description):
    """Create a STAC Collection and save it."""
    print(spatial_extent, temporal_extent)
    collection = pystac.Collection(
        id=eumet_id,
        description=description,
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([spatial_extent]),
            temporal=pystac.TemporalExtent([temporal_extent]),
        ),
        license="CC-BY-4.0",
        title=title,
        keywords=["NetCDF", "Climate", "Precipitation", "Extreme events"],
        providers=[
            pystac.Provider(
                name="Deutscher Wetterdienst (DWD)",
                roles=["producer", "licensor"],
                url="https://www.dwd.de/",
            )
        ],
    )
    collection.save_object(dest_href=output_path)
    print(f"Collection saved to {output_path}")


def create_stac_item_nc(netcdf_path, output_path):
    """Create a STAC Item for a given NetCDF file and save it."""
    dataset = xr.open_dataset(netcdf_path)

    # Extract metadata
    filename = os.path.basename(netcdf_path)
    datetime_obj_start, datetime_obj_end = extract_datetime_from_filename(
        filename, "nc")
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
        filename, "csv")
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
        "overview_regex": "^overview"
    }

    # Write to collection_config.json
    with open(metadata_dir + "/collection_config.json", "w") as file:
        json.dump(config, file, indent=4)


if __name__ == "__main__":

    # Get extended attributes from file
    with open("catalog_config.yaml", "r") as file:
        config = yaml.safe_load(file)
    title = config.get("title", "No title found")
    description = config.get("description", "No description found")
    eumet_id = config.get("id", "No id found")

    # Directory structure
    base_dir = os.getcwd()
    org_dir = os.path.join(base_dir, "appdata")
    coll_dir = os.path.join(base_dir, eumet_id)
    metadata_dir = os.path.join(coll_dir, "metadata")
    items_dir = os.path.join(metadata_dir, "items")
    data_dir = os.path.join(coll_dir, "data")

    # Ensure directories exist
    os.makedirs(coll_dir, exist_ok=True)
    os.makedirs(metadata_dir, exist_ok=True)
    os.makedirs(items_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)

    # Initialize spatial and temporal extents
    all_bboxes = []
    all_datetimes = []

    # Process each NetCDF file
    for file in os.listdir(org_dir):
        if file.endswith(".nc"):
            netcdf_path = os.path.join(org_dir, file)
            item_path = os.path.join(items_dir,
                                     f"{file.replace('.nc', '.json')}")

            # Create STAC Item
            dataset = xr.open_dataset(netcdf_path)
            bbox = get_spatial_extent(dataset)
            all_bboxes.append(bbox)

            datetime_obj_start, datetime_obj_end =\
                extract_datetime_from_filename(file, "nc")
            year_dir = os.path.join(data_dir, str(datetime_obj_start.year))
            os.makedirs(year_dir, exist_ok=True)
            file_struct = os.path.join(year_dir, eumet_id + "_" +
                                       datetime_obj_start.
                                       strftime("%Y%m%dT%H%M%S") +
                                       "_" +
                                       datetime_obj_end.
                                       strftime("%Y%m%dT%H%M%S"))
            os.makedirs(file_struct, exist_ok=True)
            shutil.copy(netcdf_path, file_struct)
            create_item_config(file_struct, netcdf_path, "nc")
            all_datetimes.append(datetime_obj_start)
            all_datetimes.append(datetime_obj_end)

            # create_stac_item_nc(netcdf_path, item_path)

        if file.endswith(".csv"):
            netcdf_path = os.path.join(org_dir, file)
            item_path = os.path.join(items_dir,
                                     f"{file.replace('.csv', '.json')}")

            # Create STAC Item
            dataset = pd.read_csv(netcdf_path, delimiter=';', comment='#')
            dataset['lat_center'] = pd.to_numeric(
                dataset['lat_center'], errors='coerce')
            dataset['lon_center'] = pd.to_numeric(
                dataset['lon_center'], errors='coerce')
            bbox = get_spatial_extent_csv(dataset)
            all_bboxes.append(bbox)

            datetime_obj_start, datetime_obj_end = \
                extract_datetime_from_filename(file, "csv")
            year_dir = os.path.join(data_dir, str(datetime_obj_start.year))
            os.makedirs(year_dir, exist_ok=True)
            file_struct = os.path.join(year_dir, eumet_id + "_" +
                                       datetime_obj_start.
                                       strftime("%Y%m%dT%H%M%S") +
                                       "_" +
                                       datetime_obj_end.
                                       strftime("%Y%m%dT%H%M%S"))
            os.makedirs(file_struct, exist_ok=True)
            shutil.copy(netcdf_path, file_struct)
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
        min(all_datetimes),
        max(all_datetimes),
    ]
    # Create STAC Collection
    collection_path = os.path.join(metadata_dir, "collection.json")
    create_stac_collection(collection_path, spatial_extent,
                           temporal_extent, eumet_id, title, description)
    create_config_json(eumet_id, metadata_dir)
