import config
import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import date, datetime
from dateutil.parser import parse
import pyodbc
import pandas as pd


def main():
    fd_param = {
      'updated_since' : "2018-06-01T00:00:00Z"
    }

  # grabs all the tickets that were changed since the "updated from time"
    r = requests.get("https://"+ config.fd_domain +".freshdesk.com/api/v2/tickets/", params=fd_param, auth = (config.api_key, config.fd_password))

    if r.status_code == 200:
      print("Request completed successfully")
      #response = json.loads(r.content)
      #print(json.dumps(response, indent=1))
      with open('tickets.txt', 'w') as outfile:
        response = json.loads(outfile)

    else:
      print("Failed to create ticket, errors are displayed below,")
      response = json.loads(r.content)
      print(response["errors"])
      print("x-request-id : " + r.headers['x-request-id'])
      print("Status Code : " + str(r.status_code))

    # call json.loads() to parse response
    #response = json.loads(r.content)

    # grabs only the ticketid from the json response and places it in a list.
    # id_list = []
    # i = 0
    # for i in range(len(response)):
    #   ticketid = response[i]['IssueID']
    #   id_list.append(ticketid)
    #   i+=1

    # loops through the list of last_updated ticket ids. Parse through each field name of the ticket and maps it to the name in SQL 
    # for id in id_list:
    #   p_id = requests.get("https://"+config.fd_domain+".jitbit.com/helpdesk/api/Ticket?id="+str(id), params=jb_param,auth=HTTPBasicAuth(config.jb_username, config.jb_password))
      
    #   if p_id.status_code == 200:

main()