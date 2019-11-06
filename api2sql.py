import config
import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import date, datetime
from dateutil.parser import parse
import pyodbc
import pandas as pd
import execute_time as ex


def main():

  # start_time creates a timestamp of when the script was executed. It will be written in a file called execute.py at the end of this script
  start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

  # last_execute is the timestamp from the last time the script was run
  last_executed = ex.execute_time

  jb_param = {
      #'mode':'unclosed',
      #'count':'100',
      'updatedFrom' : last_executed
  }

  # grabs all the tickets that were changed since the "updated from time"
  p = requests.get("https://"+config.jb_domain+".jitbit.com/helpdesk/api/Tickets?", params=jb_param, auth=HTTPBasicAuth(config.jb_username, config.jb_password))

  if p.status_code == 200:
    print("Request completed successfully")
    response = json.loads(p.content)
    # print(last_executed)
    # print(json.dumps(response, indent=1))


  else:
    print("Failed to create ticket, errors are displayed below,")
    response = json.loads(p.content)
    print(response["errors"])

    print("x-request-id : " + p.headers['x-request-id'])
    print("Status Code : " + str(p.status_code))

  # call json.loads() to parse response
  response = json.loads(p.content)

  # grabs only the ticketid from the json response and places it in a list.
  id_list = []
  i = 0
  for i in range(len(response)):
    ticketid = response[i]['IssueID']
    id_list.append(ticketid)
    i+=1

  priority_map = {
    -1 : 'Low', 0 : 'Normal',
     1 : 'High', 2 : 'Critical'}
     
  # loops through the list of last_updated ticket ids. Parse through each field name of the ticket and maps it to the name in SQL 
  for id in id_list:
    p_id = requests.get("https://"+config.jb_domain+".jitbit.com/helpdesk/api/Ticket?id="+str(id), params=jb_param,auth=HTTPBasicAuth(config.jb_username, config.jb_password))

    if p_id.status_code == 200:
      tix_response = json.loads(p_id.content)
      ticketid = tix_response['TicketID']
      priority = tix_response['Priority']
      statusid = tix_response['StatusID']
      createdate = tix_response['IssueDate']
      subject = tix_response['Subject']
      status = tix_response['Status']
      #custupdate = tix_response['UpdatedByUser']
      #agentupdate = tix_response['UpdatedByPerformer']
      categoryid = tix_response['CategoryID']
      custusername = tix_response['SubmitterUserInfo']['FullName']
      try:
        agent = tix_response['AssigneeUserInfo']['FullName']
      except TypeError:
        agent = None
      cust_fn = tix_response['SubmitterUserInfo']['FirstName']
      cust_ln = tix_response['SubmitterUserInfo']['LastName']
      duedate = tix_response['DueDate']
      try:
        agent_fn = tix_response['AssigneeUserInfo']['FirstName']
      except TypeError:
        agent_fn = None
      try:
        agent_ln = tix_response['AssigneeUserInfo']['LastName']
      except TypeError:
        agent_ln = None
      lastupdate = tix_response['LastUpdated']
      custid = tix_response['SubmitterUserInfo']['UserID']
      companyid = tix_response['SubmitterUserInfo']['CompanyId']
      companyname = tix_response['SubmitterUserInfo']['CompanyName']
      try:
        assignedto = tix_response['AssigneeUserInfo']['FullName']
      except TypeError:
        assignedto = None
      resolvedate = tix_response['ResolvedDate']
      categoryname = tix_response['CategoryName']
      cust_email = tix_response['SubmitterUserInfo']['Email']

      # Reassign data types to extracted data points

      ticketid = str(ticketid)
      priority = int(priority)
      createdate = str(createdate)
      subject = str(subject)
      status = str(status)
      custusername = str(custusername)
      agent = str(agent)
      cust_fn = str(cust_fn)
      cust_ln = str(cust_ln)
      duedate = str(duedate)
      agent_fn = str(agent_fn)
      agent_ln = str(agent_ln)
      lastupdate = str(lastupdate)
      custid = str(custid)
      companyid = str(companyid)
      companyname = str(companyname)
      assignedto = str(assignedto)
      resolvedate = str(resolvedate)
      categoryname = str(categoryname)
      try:
        category, detail, = categoryname.split("/", 1)
      except ValueError:
        category, detail = categoryname, None
      cust_email = str(cust_email)

      # Parse date variables to make updated variable a date data type

      createdate_1 = parse(createdate, fuzzy=True)
      updatedate = parse(lastupdate, fuzzy=True)
      try:
        resolvedate_1 = parse(resolvedate, fuzzy=False, default= None)
      except ValueError:
        resolvedate_1 = None
      try:
        duedate_1 = parse(duedate, fuzzy=False, default= None)
      except ValueError:
        duedate_1 = None

      # Parse CategoryId field to get info from custom field. (JitBit has a seperate API call needed for custom fields)

      get_custfields = requests.get("https://"+config.jb_domain+".jitbit.com/helpdesk/api/TicketCustomFields?id="+ticketid,auth=HTTPBasicAuth(config.jb_username, config.jb_password))
      if get_custfields.status_code == 200:
        customfield_response = json.loads(get_custfields.content)
        arch_priority = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Archer Priority']
        casename = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Case Name']
        agency_col = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Agency/Collector Associated with Task']
        req_size = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Request Size']
        arch_status = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Archer Status']
        fd_id = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Freshdesk Ticket Id']
        ticket_diff = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Ticket Difficulty']
        process_time = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Processing Tim']
      
      # Assign data type for custom fields
        
        try:
          arch_priority = str(arch_priority[0])
        except IndexError:
          arch_priority = None
        try:
          casename = str(casename[0])
        except IndexError:
          casename = None
        try:
          agency_col = str(agency_col[0])
        except IndexError:
          agency_col = None
        try:
          req_size = str(req_size[0])
        except IndexError:
          req_size = None
        try:
          arch_status = str(arch_status[0])
        except IndexError:
          arch_status = None
        try:
          fd_id = str(fd_id[0])
        except IndexError:
          fd_id = None
        try:
          ticket_diff = str(ticket_diff[0])
        except IndexError:
          ticket_diff = None
        try:
          process_time = str(process_time[0])
        except IndexError:
            process_time = None

      # Connect to SQL

      connection = pyodbc.connect(
      r'DRIVER={SQL Server Native Client 11.0};'
      r'SERVER=' + config.server + ';'
      r'DATABASE=' + config.database + ';'
      r'UID=' + config.username + ';'
      r'PWD=' + config.password
      )

      cursor = connection.cursor()
      
      # Create JitBit backup table
      jitbit_backup = 'DROP TABLE IF EXISTS JitBit_BackUp; SELECT * INTO JitBit_BackUp FROM JitBit'
      cursor.execute(jitbit_backup)

      # Check existence of ticket in SQL

      sql_insert = ('INSERT INTO JitBit (TicketId,	Priority,	Subject,	Status,	Cust_Username,	Agent,	Cust_FirstName,	Cust_LastName,	Agent_FirstName,	Agent_LastName,	CustId,	CompanyId,	CompanyName,	AssignedTo,	Category, Detail,	Cust_Email,	CreateDate, DueDate, LastUpdateDate, ResolveDate, Archer_Priority, CaseName, Agency_Collector, Request_Size, Archer_Status, FD_TIcketId, Ticket_Difficult, Processing_Time) values (?, ?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?)')
      cursor.execute(sql_insert, (ticketid, priority_map[priority], subject, status, custusername, agent, cust_fn, cust_ln, agent_fn, agent_ln, custid, companyid, companyname, assignedto, category, detail, cust_email, createdate_1, duedate_1, updatedate, resolvedate_1, arch_priority, casename, agency_col, req_size, arch_status, fd_id, ticket_diff, process_time))
      sql_dedup = ('exec JitBit_DeDup')
      cursor.execute(sql_dedup)
      cursor.commit()
    
    else:
      print("GET response failed. Try again")
  f = open('execute_time.py', 'w')
  # log the start time of this script to a seperate file which will be called on next time the script is run.
  f.write('execute_time =' + '"' + start_time + '"')

main()

