#!/usr/bin/env python3 
# -*- coding: utf-8 -*- 

"""This script will preprocess the data for the model.
TODO: Add filter portion of the script.
TODO: Option to squelch logging.
"""

import argparse
import zipfile
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("NSAC-Preprocessor")

def unzip_file(file_path, output_dir):
    """
    Unzip a file.

    :param file_path: The path to the file to unzip.
    :param output_dir: The path to the output directory.
    return: None
    """
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)

def unzip(input_dir_path):
    """
    Unzip all the files in the input directory.

    :param input_dir_path: The path to the input directory.
    :param type: str
    :return: None
    """
    for dir in os.listdir(input_dir_path):
        subdir_path = os.path.join(input_dir_path, dir)
        for file in os.listdir(subdir_path):
            if file.endswith(".zip"):
                logger.log(logging.Info, "Unzipping file: {}".format(file))
                full_path = os.path.join(subdir_path, file)
                
                try:
                    unzip_file(full_path, full_path[:-4])
                except zipfile.BadZipFile as e: 
                    if(logger):
                        logger.error("Bad zip file: {}".format(file))
                    else:
                        print(f"Unable to unzip file {full_path}")
                    

def main(args):
    input_dir_path = args.input_dir
    output_dir_path = args.output_dir

    unzip(input_dir_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Preprocess the data for the model by unzipping and filtering.')
    parser.add_argument('-i', '--input_dir', type=str, help='The directory of the input data.', default="data/raw_maps")
    parser.add_argument('-o', '--output_dir', type=str, help='The directory of the filtered data.', default="data/filtered_maps")
   
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        logger.log(logging.INFO, f"Creating output directory {args.output_dir}")
        os.makedirs(args.output_dir)

    main(args)