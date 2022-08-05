#!/usr/bin/env python3

import requests
import json, logging, csv, sys, re, json
from urllib.request import urlopen
from lxml import html


logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logging.info("Creating CSV writer, and writing headers to standard out.")
csv_data = []
writer = csv.writer(sys.stdout)
headers = ["service_name","service_id","sku_name","sku_id","sku_resource_family",
    "sku_resource_group","sku_usage_type","sku_region","sku_vm_family"]
writer.writerow(headers)

logging.info("Getting SKU group data by scraping HTML tables.")
sku_vm_family_map = {}
try:
    groups_url = "https://cloud.google.com/skus/sku-groups"
    groups_page = urlopen(groups_url)
    groups_content = groups_page.read()
    groups_xml = html.fromstring(groups_content)
    groups = [{'name': cell.find(".//a").text,"url": "https://cloud.google.com"+cell.find(".//a").get("href")} for cell in groups_xml.findall(".//td")]

    logging.info("Parsing content for each sku group.")
    for group in groups:
        group_name = group["name"]
        group_url = group["url"]

        vm_family_match = re.match('.*([A-Z]{1}[0-9]{1}[A-Z]{0,1}) VMs.*',group_name)
        if vm_family_match == None:
            continue

        vm_family = vm_family_match.groups()[0]

        logging.info("Parsing content for '{}' sku group.".format(group_name))
        skus_page = urlopen(group_url)
        skus_content = skus_page.read()
        skus_xml = html.fromstring(skus_content)
        rows_xml = [row for row in skus_xml.findall(".//tr")[1:]]

        logging.info("Parsing content for each column.")
        for row_xml in rows_xml:
            cols_xml = row_xml.findall(".//td")
            sku_id = cols_xml[2].find(".//a").text
            logging.info("Mapping sku ID {} to VM family {}.".format(sku_id, vm_family))
            sku_vm_family_map[sku_id] = vm_family

except Exception as e:
    logging.error("Failed to get SKU groups: {}".format(e))

logging.info("Getting key.")
try:
    key = open('key.secret').read()
    next_page_token = ""
    params = {"key": key}
    params = {"key": key, "pageToken": next_page_token}
except Exception as e:
    logging.error("Failed to get key: {}".format(e))
    exit(1)

logging.info("Getting services list.")
try:
    services = []
    services_url = "https://cloudbilling.googleapis.com/v1/services"

    while True:
        logging.info("Getting services page token: {}".format(next_page_token))
        services_response = requests.get(services_url, params=params)
        services_response_data = json.loads(services_response.content)
        next_page_token = services_response_data.get("nextPageToken")
        services = services + services_response_data.get("services")
        params = {"key": key, "pageToken": next_page_token}
        if next_page_token == "":
            break
    
    logging.info("Found {} services.".format(len(services)))

except Exception as e:
    logging.error("Failed to get service list: {}".format(e))
    exit(1)

logging.info("Getting sku details.")
try:
    for service in services:
        service_id = service.get("serviceId")
        sku_url = "https://cloudbilling.googleapis.com/v1/services/{}/skus".format(service_id)
        next_page_token = None

        logging.info("Getting skus for service {}".format(service.get("displayName")))
        while True:
            params = {"key": key, "pageToken": next_page_token}
            sku_response = requests.get(sku_url, params=params)
            sku_response_data = json.loads(sku_response.content)
            next_page_token = sku_response_data.get("nextPageToken")

            logging.info("Getting skus page token: {}".format(next_page_token))
            skus_data = sku_response_data.get("skus")
            if skus_data in ["",None]:
                continue

            logging.info("Found {} skus for service {}.".format(len(skus_data), service.get("displayName")))

            logging.info("Flattening service and sku data.")
            for sku in skus_data:
                try:
                    sku_category = sku.get("category")
                    for location in sku.get("serviceRegions"):
                        row = [ 
                            service.get("displayName"), # service_name
                            service_id, 
                            sku.get("description"), # sku_name
                            sku.get("skuId"), # sku_id
                            sku_category.get("resourceFamily"), # sku_resource_family
                            sku_category.get("resourceGroup"), # sku_resource_group
                            sku_category.get("usageType"), #sku_usage_type
                            location, # sku_region
                            sku_vm_family_map.get(sku.get("skuId"))
                        ]
                        writer.writerow(row)

                except Exception as e:
                    logging.error("Failed to flatten data: {}".format(e))
                    
                    
            if next_page_token in ["",None]:
                break



except Exception as e:
    logging.error("Failed to get sku details: {}".format(e))
    exit(1)
