#!/usr/bin/env python3

# Copyright (C) 2016 Institute of Computer Science of the Foundation for Research and Technology - Hellas (FORTH)
# Authors: Michalis Bamiedakis, Dimitris Mavrommatis and George Nomikos
#
# Contact Author: George Nomikos
# Contact Email: gnomikos [at] ics.forth.gr
#
# This file is part of traIXroute.
#
# traIXroute is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3.
#
# traIXroute is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with traIXroute.  If not, see <http://www.gnu.org/licenses/>.

from urllib.request import urlretrieve, urlopen
from traixroute.handler import handle_json
import shutil
import ujson
import os
import shutil
import subprocess
import concurrent.futures
import sys
import json
import re


class download_files():

    '''
    Downloads all the files to construct the core database appropriate for path analysis.
    '''

    def __init__(self, config, destination_path):
        '''
        Sets the urls to the pdb, pch and routeviews datasets for downloading, imported from the config file.
        Input:
            a) config: Dictionary that contains the data in the config file.
        '''
        self.ixpfx = config["peering"]["ixp_pfx_link"]
        self.ix = config["peering"]["ix_link"]
        self.netixlan = config["peering"]["netixlan_link"]
        self.ixlan = config["peering"]["ixplan_link"]

        #self.ixp_exchange = config["pch"]["ixp_exchange"]
        self.ixp_exchange = 'https://www.pch.net/api/ixp/directory'
        self.ixp_ip = config["pch"]["ixp_ips"]
        self.ixp_subnet = config["pch"]["ixp_subnet"]
        self.pch_threads = 20 #number of threads for pch files download
        self.caida_log = config["caida_log"]

        self.homepath = destination_path

    def start_download(self):
        '''
        Downloads and checks whether all the needed files have been downloaded successfully.
        Output:
            a) True if the files have been downloaded successfully, False otherwise.
        '''

        if os.path.exists(self.homepath + '/database'):
            shutil.rmtree(self.homepath + '/database')
        os.makedirs(self.homepath + '/database')
        os.makedirs(self.homepath + '/database/PCH')
        os.makedirs(self.homepath + '/database/PDB')
        os.makedirs(self.homepath + '/database/RouteViews')
        # Setting shared variables to use multiple processes.
        peering = False
        pch = False
        routeviews = False

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            peering = executor.submit(
                self.download_peering, 0)
            pch = executor.submit(self.download_pch, 0)
            routeviews = executor.submit(
                self.download_routeviews)

        if routeviews.result() and peering.result() and pch.result():
            with open(self.homepath + "/configuration/check_update.txt", "w") as f:
                f.write("1")
            return True
        else:
            return False

    def download_peering(self, option):
        '''
        Downloads the peeringdb .json files.
        Input:
            b) option: Flag to select file(s) to download.
        Output:
            a) True if the files have been downloaded successfully, False otherwise.
         '''

        print('Started downloading PDB dataset.')
        try:
            if option == 1 or not option:
                request = self.ixpfx
                response = urlopen(request)
                str_response = response.read().decode('utf-8')
                obj = ujson.loads(str_response)

                with open(self.homepath + '/database/PDB/ixpfx.json', 'w') as f:
                    ujson.dump(obj, f)

            if option == 2 or not option:
                request = self.ix
                response = urlopen(request)
                str_response = response.read().decode('utf-8')
                obj = ujson.loads(str_response)

                with open(self.homepath + '/database/PDB/ix.json', 'w') as f:
                    ujson.dump(obj, f)

            if option == 3 or not option:
                request = self.netixlan
                response = urlopen(request)
                str_response = response.read().decode('utf-8')
                obj = ujson.loads(str_response)

                with open(self.homepath + '/database/PDB/netixlan.json', 'w') as f:
                    ujson.dump(obj, f)

            if option == 4 or not option:
                request = self.ixlan
                response = urlopen(request)
                str_response = response.read().decode('utf-8')
                obj = ujson.loads(str_response)

                with open(self.homepath + '/database/PDB/ixlan.json', 'w') as f:
                    ujson.dump(obj, f)
        except Exception as e:
            print('PDB exception:' + str(e))
            print('PDB dataset cannot be updated.')
            return False
        print('PDB dataset has been updated successfully.')
        return True

    def download_pch(self, option):
        '''
        Downloads the PCH files.
        Input:
            b) option: Flag to select file(s) to download.
        Output:
            a) True if the files have been downloaded successfully, False otherwise.
        '''

        print('Started downloading PCH dataset.')
        try:
            json_handle_local = handle_json.handle_json()
            # add option to download only needed files?
            request = self.ixp_exchange
            print(self.ixp_exchange)
            response = urlopen(request)
            str_response = response.read().decode('utf-8')
            obj = ujson.loads(str_response)

            # urlretrieve(self.ixp_exchange, self.homepath + '/database/PCH/ixp_exchange.json')
            print("IXP Exchange downloaded")

            json_handle_local.export_IXP_dict(obj, self.homepath + '/database/PCH/ixp_exchange.json')


            # testing without download:
            # with open("ixp_exchange.json") as f:
            #    obj = ujson.load(f)

            active_ixps = {}
            subnet_urls = []
            membership_urls = []
            ixp_ids = []

            if not os.path.exists(self.homepath + '/database/PCH/temp_files'):
                os.mkdir(self.homepath + '/database/PCH/temp_files')

            for ixp in obj:
                if ixp['stat'] == 'Planned' or ixp['stat'] == 'Active':
                    active_ixps[ixp['id']] = ixp
                    if not os.path.exists(self.homepath + '/database/PCH/temp_files/subnet_' + str(ixp['id']) + '.json')\
                            or not os.path.exists(self.homepath + '/database/PCH/temp_files/membership_' + str(ixp['id']) + '.json'):
                        subnet_urls.append(self.ixp_subnet + ixp['id'])
                        membership_urls.append(self.ixp_ip + ixp['id'])
                        ixp_ids.append(ixp['id'])
                    else:
                        print('File for IXP id ' + str(ixp['id']) + ' already downloaded')
            #with open(self.homepath + '/database/PCH/temp_files/test_file.json', 'w') as f:
            #    ujson.dump(active_ixps, f)



            if len(ixp_ids) > 0:
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.pch_threads) as executor:
                    result = executor.map(self.get_files, ixp_ids)

            subnet_dict = {}
            membership_dict = {}

            for ixp in active_ixps:
                #print(item)
                ixp_id = active_ixps[ixp]['id']
                [subnet_dict[ixp_id], sub_flag] = json_handle_local.import_IXP_dict(self.homepath +
                                                        '/database/PCH/temp_files/subnet_' + str(ixp_id) + '.json')
                [membership_dict[ixp_id], mem_flag] = json_handle_local.import_IXP_dict(self.homepath +
                                                        '/database/PCH/temp_files/membership_' + str(ixp_id) + '.json')

            json_handle_local.export_IXP_dict(subnet_dict, self.homepath + '/database/PCH/ixp_subnets.json')
            json_handle_local.export_IXP_dict(membership_dict, self.homepath + '/database/PCH/ixp_membership.json')

            print('All tasks completed')

            #delete temp_files directory
            #shutil.rmtree(path + '/temp_files')

        except Exception as e:
            print('PCH exception' + str(e))
            print('PCH dataset cannot be updated')
            return False
        print('PCH dataset has been updated successfully.')

        return True

    def get_files(self, ixp_id):
        try:
            urlretrieve(self.ixp_subnet + str(ixp_id),
                        self.homepath + '/database/PCH/temp_files/subnet_' + str(ixp_id) + '.json')
            print('Requested URL: ' + self.ixp_subnet + str(ixp_id))
            urlretrieve(self.ixp_ip + str(ixp_id),
                        self.homepath + '/database/PCH/temp_files/membership_' + str(ixp_id) + '.json')
            print('Requested URL: ' + self.ixp_ip + str(ixp_id))
        except Exception as e:
            print('ERROR with IXP: ' + ixp_id)
            print(str(e))
        return True

    def download_routeviews(self):
        '''
        Downloads the Routeviews AS-to-Subnet file.
        Output:
             a) True if the files have been downloaded successfully, False otherwise.
        '''

        print('Started downloading RouteViews dataset.')
        # Downloads the log file to find the last version of the routeviews
        # file.
        try:
            urlretrieve(self.caida_log, self.homepath +
                        '/database/RouteViews/caidalog.log')
        except Exception as e:
            print(str(e))
            print('RouteViews dataset cannot be updated.')
            return False

        # Parses the log file to find the file name.
        try:
            f2 = open(self.homepath + '/database/RouteViews/caidalog.log')
        except Exception as e:
            print(str(e))
            print('RouteViews cannot be updated.')
            return False

        updates = f2.read()
        f2.close()
        updates = updates.split('\n')
        updates = updates[len(updates) - 2].split('\t')[2]

        # Downloads and extracts the routeviews file.
        try:
            urlretrieve('http://data.caida.org/datasets/routing/routeviews-prefix2as/' +
                        updates, self.homepath + '/database/RouteViews/routeviews.gz')
        except Exception as e:
            print(str(e))
            print('RouteViews cannot be updated.')
            return False

        try:
            subprocess.call(
                str('gunzip ' + self.homepath + '/database/RouteViews/routeviews.gz').split(" "), shell=False)
        except Exception as e:
            print(str(e))
            print('RouteViews cannot be updated.')
            return False

        if os.path.exists(self.homepath + '/database/RouteViews/routeviews.gz'):
            os.remove(self.homepath + '/database/RouteViews/routeviews.gz')
        if os.path.exists(self.homepath + '/database/RouteViews/caidalog.log'):
            os.remove(self.homepath + '/database/RouteViews/caidalog.log')
        print('Routeviews has been updated successfully.')

        return True

    def getDestinationPath(self):
        return self.homepath
