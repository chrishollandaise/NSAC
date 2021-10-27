#!/usr/bin/env python3 
# -*- coding: utf-8 -*- 

"""This script will download the latest n_maps of the map data from BeatSaver API.

# TODO: This script could be improved by using the BeatSaver API's web socket endpoint.
# TODO: Parallelizing the request and download process can improve the performance.
# TODO: Can make use of a cache to avoid sending requests to endpoints that the registry might already cover.
# TODO: Can make use of a cache records to avoid scanning the output directory for maps.
# TODO: Another improvement can be to add a progress bar to the download process.
# TODO: Use some heuristic to determine the most probable before time parameter, so it doesn't need to make a trace back all dates from the present to the past.
# TODO: Option to squelch the logger.
"""

import requests
import json
import time
from datetime import datetime
import argparse
import os, sys
import logging 

# the time the script started
NOW = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

class BSDownloader():
    """
    Downloads maps and meta information from the BeatSaver API. It also manages the download directory. 
    Supports requests only to the /maps/latest endpoint.

    :param before: the time to determine which maps to download from
    :type before: str
    :param n_maps: number of maps to download
    :type n_maps: int
    :param output_dir: directory to download maps to
    :type output_dir: str
    """

    # the name of the meta file
    _META_FILE = 'meta.json'
    # the request delay
    DELAY = 0

    def __init__(self, before, n_maps, output_dir):
        """ Constructor method
        """
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__class__.__name__)
        self.n_maps = n_maps
        self.before = before
        # the original before timestamp
        self.org_before = before
        self.output_dir = output_dir
        self.maps = {**self._get_existing_maps()}

    def download_latest(self, params={'before': '2019-07-21T00:34:41.775Z', 'auto_mapper': False, 'sort': 'LAST_PUBLISHED'}):
        """
        Downloads the latest maps from the BeatSaver API.

        :param params: parameters to pass to the API
        :type params: dict
        :return: number of downloaded maps from this run
        :rtype: int
        """
        count = 0
        # flag used to determine if the downloader should continue to download maps
        done_flag = False
        LATEST_ENDPOINT = "https://api.beatsaver.com/maps/latest"
        self.logger.log(logging.INFO, "Downloading latest maps...")

        while done_flag == False:
            response = self._requestJSON(LATEST_ENDPOINT, params)

            # if the response is empty, the downloader has finished downloading all the maps
            if(len(response['docs']) <= 0): 
                done_flag = True; 
                break;

            for map_JSON in response['docs']:
                if map_JSON['id'] not in self.maps:
                    self.maps[map_JSON['id']] = self._write_meta_file(map_JSON)
                    self._downloadMap(map_JSON)
                    # if the map is not in the registry, update the before parameter
                    
                    # if the number of maps downloaded is equal to the number of maps to download, the downloader has finished downloading all the maps
                    if count > self.n_maps:
                        done_flag = True; 
                        break;

                    count += 1

                else:
                    self.logger.log(logging.INFO, "Map {} already downloaded".format(map_JSON['id']))
            
            if done_flag == True: break;

            # update the before parameter
            before = response['docs'][-1]['lastPublishedAt']

            self.logger.log(logging.INFO, "Current maps downloaded: {}/{} maps".format(count, "∞" if self.n_maps == sys.maxsize else self.n_maps))

            self.logger.log(logging.INFO, "Requesting for more maps before {}".format(before))
            params = {**params, 'before': before}

            # wait for a bit before requesting more maps
            time.sleep(self.DELAY)

        
        return count

    def _finish_downloading(self, meta_path):
        """
        If a map directory has been determined not to have finished downloading all its files, it will finish downloading for that map.
        """

        # iterate through all the maps meta paths
        meta_JSON = json.load(open(meta_path))

        level_path = meta_JSON["versions"][0]["downloadURL"].split('/')[-1]
        # if the map has not finished downloading, finish downloading it
        if not os.path.exists(os.path.join(os.path.dirname(meta_path), level_path)):
            self.logger.log(logging.INFO, "Map {} is missing its level file".format(meta_JSON['id']))
            self._downloadMap(meta_JSON)
                

    def _downloadMap(self, map_JSON):
        """
        Performs the actual download of the map.

        :param map_JSON: the map's meta data provided by the BeatSaver API
        :type map_JSON: dict
        """
        map_dir = os.path.join(self.output_dir, map_JSON['id'])
        # if the map directory does not exist, create it
        if not os.path.exists(map_dir): os.mkdir(map_dir)
        
        download_url = map_JSON["versions"][0]['downloadURL']
        file_name = download_url.split('/')[-1]

        self.logger.log(logging.INFO, "Downloading levels file associated with map {}".format(map_JSON['id']))
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()

            # write the level file to the map directory
            with open(os.path.join(map_dir, file_name), 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk: f.write(chunk)
    
    def _write_meta_file(self, map_JSON):
        """
        Writes the meta file for the map.

        :param map_JSON: the map's meta data provided by the BeatSaver API
        :type map_JSON: dict
        """
        map_dir = os.path.join(self.output_dir, map_JSON['id'])
        if not os.path.exists(map_dir): os.mkdir(map_dir)

        self.logger.log(logging.INFO, "Writing meta file for map: {}".format(map_JSON['id']))
        with open(os.path.join(map_dir, self._META_FILE), 'w') as f:
            json.dump(map_JSON, f)
        
        return(os.path.join(map_dir, self._META_FILE))
        
    def _get_existing_maps(self):
        """
        Gets the existing maps in the output directory. 

        :return: a dictionary of the existing maps
        :rtype: dict
        """
        existing_maps = {}
        self.logger.log(logging.INFO, "Checking for existing maps...")
        with os.scandir(self.output_dir) as levels:
            # iterate through all the directories in the output directory
            for level in levels:
                if level.is_dir():
                    with os.scandir(level) as files:
                        # iterate through all the files in the directory
                        for file in files:
                            # if the file is a meta file, add the map to the existing maps dictionary
                            if file.is_file() and file.name == self._META_FILE:
                                self.logger.log(logging.INFO, "Found prexisting map at {}".format(level.path))
                                existing_maps[level.name] = file.path

                                if len(os.listdir(os.path.dirname(file.path))) < 2:
                                    self.logger.log(logging.INFO, "Map {} is missing its level file".format(level.name))
                                    self._finish_downloading(file.path)

        
        return existing_maps
    

    def _requestJSON(self, ENDPOINT, params):
        """
        A generic method to request JSON from an endpoint.
        
        :param ENDPOINT: the endpoint to request from
        :type ENDPOINT: str
        :param params: the parameters to pass to the endpoint
        :type params: dict
        :return: the JSON response
        """
        
        self.logger.log(logging.INFO, "Requesting for JSONs from {} endpoint".format(ENDPOINT))
        r = requests.get(ENDPOINT, params=params)
        r.raise_for_status()

        return r.json()

def main(before, n_maps, output_dir):
    downloader = BSDownloader(before, n_maps, output_dir)

    count = downloader.download_latest()
    print(f"Finished scraping maps. A total of {count} maps were downloaded.")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Downloads maps from BeatSaver', usage=f'python3 {sys.argv[0]} -n <number of maps> -o <output directory> -b <before time parameter>')
    parser.add_argument('-n', '--n_maps', type=int, default=sys.maxsize, help='The number of maps to download, by default its a very large positive number', required=False)
    parser.add_argument('-o', '--output-dir', type=str, help='The directory to download the maps to', required=True)
    parser.add_argument('-b', '--before-param', type=str, default=NOW, 
                        help='The before parameter to pass to the BeatSaver API, downloads maps BEFORE the specified date.', required=False)

    args = parser.parse_args()

    main(args.before_param, args.n_maps, args.output_dir)