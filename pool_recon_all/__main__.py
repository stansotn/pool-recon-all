"""
pool-recon-all

Multiprocessing pool for freesurfer's recon all command.
Requires a csv file with .nii id and path information (see nii_path.py)

- Stanislav Sotnikov (ssotnikov@ccny.cuny.edu)
- The City College of New York
"""


import multiprocessing
import os
import pathlib
import sys
import subprocess
import logging
import time


import tqdm
import pandas as pd
import click


# Set up logging
log_format = "[%(levelname)-8s] %(name)-24s %(message)s"
logging.basicConfig(level=logging.DEBUG, format=log_format)
logger = logging.getLogger(__file__)


def worker_process(args: tuple):

    nii_id, nii_path, descriptor_path = args
    # Mark worker start time and release memory
    df_descr = pd.read_csv(descriptor_path, index_col="Image Data ID")
    start_t = time.strftime("%Y-%m-%d-%H:%M:%S")
    df_descr.loc[nii_id, "time_start"] = start_t
    df_descr.to_csv(descriptor_path)

    # Run recon-all
    logger.info("Started ID{} at {}".format(nii_id, start_t))
    recon_all_cmd = ["recon-all", "-all", "-subject", nii_id, "-i", nii_path]
    subprocess.run(
        recon_all_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    # Mark worker end time
    stop_t = time.strftime("%Y-%m-%d-%H:%M:%S")
    df_descr.loc[nii_id, "time_stop"] = stop_t
    logger.info("Finished ID{} at {}".format(nii_id, stop_t))


@click.command()
@click.option("--dataset-dir", "-d", type=str, required=True)
@click.option("--output-dir", "-o", type=str, required=True)
def recon(dataset_dir: str, output_dir: str):

    dataset_dir = pathlib.Path(dataset_dir)
    output_dir = pathlib.Path(output_dir)

    logger.info(os.environ["FREESURFER"])
    logger.info(os.environ["SUBJECTS_DIR"])

    if not (
        os.environ["FREESURFER"]
        and pathlib.PurePath(os.environ["SUBJECTS_DIR"]) == output_dir
    ):
        logger.critical("Check environmental variables.")
        sys.exit()

    if not all([dataset_dir.is_dir(), dataset_dir.is_dir()]):
        logger.critical("Arguments are not directories")
        sys.exit()

    descriptor_file = output_dir / (dataset_dir.parts[-1] + "-processed.csv")

    if not descriptor_file.is_file():
        logger.critical(
            "Descriptor file not found:\n{}".format(str(descriptor_file))
        )
        sys.exit()

    df_descr = pd.read_csv(str(descriptor_file), index_col="Image Data ID")

    df_todo = df_descr[
        (df_descr["time_start"].isnull())
        & (df_descr["time_stop"].isnull())
        & (df_descr["success"].isnull())
    ]

    print(df_todo)

    args = (
        (i, str(dataset_dir / df_todo.loc[i, "path"]), str(descriptor_file))
        for i in list(df_todo.index)
    )

    with multiprocessing.Pool(processes=12) as pool:
        with tqdm.tqdm(total=len(df_descr)) as pbar:
            pbar.update(len(df_descr) - len(df_todo))
            for _ in pool.imap_unordered(worker_process, args):
                pbar.update(1)


if __name__ == "__main__":
    recon()
