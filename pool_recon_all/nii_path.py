"""
nii_path.py

Constructs a table of paths for .nii data collections
Removes duplicates and single visits

- Stanislav Sotnikov (ssotnikov@ccny.cuny.edu)
- The City College of New York
"""


import pathlib
import logging
import os

from tqdm import tqdm
import pandas as pd
import click


@click.command()
@click.option(
    "--input",
    "-i",
    type=str,
    required=True,
    help="directory containing MRI data and csv summary",
)
def nii_path(input: str):

    # Set up logging
    log_format = "[%(levelname)-8s] %(name)-24s %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=log_format)
    logger = logging.getLogger(__file__)

    # Find csv file with info
    path_tree = os.walk(input)
    dir_path, _, filenames = next(path_tree)

    # Infer the descriptor table name from the underlying directory name.
    # Directory must have the same name as the csv file containing the data
    descriptor_name = dir_path[dir_path.rfind("/") + 1 :] + ".csv"

    if descriptor_name not in filenames:
        logger.error("descriptor table not found")
        return

    logger.info("loading descriptor table")
    df_adni = pd.read_csv(
        dir_path + "/" + descriptor_name, index_col="Image Data ID"
    )
    logger.info(
        "dataset descriptor file contains {} images".format(len(df_adni))
    )

    # Drop unusable columns
    df_adni.drop(
        columns=["Visit", "Type", "Modality", "Format", "Downloaded"],
        inplace=True,
    )

    # Parse dates
    df_adni["Acq Date"] = pd.to_datetime(df_adni["Acq Date"])

    # Get list of unique subjects
    subjects = df_adni["Subject"].unique()

    # Remove duplicates
    df_adni = df_adni[~df_adni["Description"].str.contains("_2")]
    logger.info(
        "{} images remain after removing duplicates".format(len(df_adni))
    )

    # Remove all subjects with single visit
    for subject in subjects:
        if len(df_adni[df_adni["Subject"] == subject]) == 1:
            df_adni = df_adni[df_adni["Subject"] != subject]
            logger.info(
                "removing subject {} with a single visit".format(subject)
            )

    # Traverse filesystem finding nii relative paths
    pbar = tqdm(total=len(df_adni))
    for dir_path, dir_names, filenames in path_tree:
        if not (len(filenames) == 1 and len(dir_names) == 0):
            continue

        path = pathlib.PurePath(dir_path)

        # Check extension
        index_ext = filenames[0].rfind(".")
        if filenames[0][index_ext:] != ".nii":
            logger.error("Inconsistency when traversing filesystem..")
            logger.error(filenames[0])
            continue

        # Determine image id beginning with 'S' and 'I'
        index_sid = filenames[0].rfind("_S")
        index_iid = filenames[0].rfind("_I")

        sid = filenames[0][index_sid + 1 : index_iid]
        iid = filenames[0][index_iid + 1 : index_ext]

        # sid should point to the end directory where
        # .nii file is located.
        if path.parts[-1] != sid:
            logger.error("Inconsistency path sid")

        # Get relative path based on the directory containing the descriptor
        relative_idx = path.parts.index(
            descriptor_name[: descriptor_name.rfind(".csv")]
        )
        relative_path = str(
            pathlib.PurePath(*path.parts[relative_idx + 1 :], filenames[0])
        )
        if iid in df_adni.index:
            df_adni.loc[df_adni.index == iid, "S ID"] = sid
            df_adni.loc[df_adni.index == iid, "path"] = relative_path
            pbar.update(1)

    pbar.close()

    # Create columns to be used by __main__.py
    df_adni["time_start"] = ""
    df_adni["time_stop"] = ""
    df_adni["success"] = ""

    df_adni.to_csv(
        input
        + "/"
        + descriptor_name[: descriptor_name.rfind(".csv")]
        + "-processed.csv",
    )


if __name__ == "__main__":
    nii_path()
