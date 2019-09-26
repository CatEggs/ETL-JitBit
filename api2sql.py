import config
import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import date
from dateutil.parser import parse
import pyodbc
import pandas as pd

# command to run it in the terminal -->python -c 'import api2sql; ticket = api2sql.iter_tickets_from_api()'

jb_param = {
    'mode':'all',
    'count':'100'
}

p = requests.get("https://"+config.jb_domain+".jitbit.com/helpdesk/api/Tickets", params=jb_param,auth=HTTPBasicAuth(config.jb_username, config.jb_password))

if p.status_code == 200:
  print("Request completed successfully")
  response = json.loads(p.content)
  print(json.dumps(response, indent=1))


else:
  print("Failed to create ticket, errors are displayed below,")
  response = json.loads(p.content)
  print(response["errors"])

  print("x-request-id : " + p.headers['x-request-id'])
  print("Status Code : " + str(p.status_code))

# Call json.loads() to parse response

response = json.loads(p.content)

# Extract the field names

i = 0
for i in range(len(response)):
    ticketid = response[i]['IssueID']
    priority = response[i]['Priority']
    statusid = response[i]['StatusID']
    createdate = response[i]['IssueDate']
    subject = response[i]['Subject']
    status = response[i]['Status']
    custupdate = response[i]['UpdatedByUser']
    agentupdate = response[i]['UpdatedByPerformer']
    categoryid = response[i]['CategoryID']
    custusername = response[i]['UserName']
    agent = response[i]['Technician']
    cust_fn = response[i]['FirstName']
    cust_ln = response[i]['LastName']
    duedate = response[i]['DueDate']
    agent_fn = response[i]['TechFirstName']
    agent_ln = response[i]['TechLastName']
    lastupdate = response[i]['LastUpdated']
    custid = response[i]['UserID']
    companyid = response[i]['CompanyID']
    companyname = response[i]['CompanyName']
    assignedto = response[i]['AssignedToUserID']
    resolvedate = response[i]['ResolvedDate']
    categoryname = response[i]['Category']
    cust_email = response[i]['Email']
    haschild = response[i]['HasChildren']
    i+=1

    # Reassign data types to extracted data points

    ticketid = str(ticketid)
    priority = str(priority)
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
    cust_email = str(cust_email)
    haschild = str(haschild)

    # Parse date variables to make updated variable a date data type

    createdate_1 = parse(createdate, fuzzy=True)
    updatedate = parse(lastupdate, fuzzy=True)
    try: 
      duedate_1 = parse(duedate, fuzzy=True)
    except ValueError:
      pass

    try: 
      resolvedate_1 = parse(resolvedate, fuzzy=True)
    except ValueError:
      pass


    # Connect to SQL

    connection = pyodbc.connect(
        r'DRIVER={SQL Server Native Client 11.0};'
        r'SERVER=' + config.server + ';'
        r'DATABASE=' + config.database + ';'
        r'UID=' + config.username + ';'
        r'PWD=' + config.password
    )
    cursor = connection.cursor()
    
    #sql_exists = ('select * from JitBit where TicketId = ?')
    sql_insert = ('INSERT INTO JitBit (TicketId,	Priority,	Subject,	Status,	Cust_Username,	Agent,	Cust_FirstName,	Cust_LastName,	Agent_FirstName,	Agent_LastName,	CustId,	CompanyId,	CompanyName,	AssignedTo,	CategoryName,	Cust_Email,	HasChildTicket, CreateDate, DueDate, LastUpdateDate, ResolveDate) values (?, ?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?, ?, ?, ?)')
    cursor.execute(sql_insert, (ticketid, priority, subject, status, custusername, agent, cust_fn, cust_ln, agent_fn, agent_ln, custid, companyid, companyname, assignedto, categoryname, cust_email, haschild, createdate_1, duedate_1, updatedate, resolvedate_1))
    #cursor.execute(sql_exists, ticketid)
    #rows = cursor.fetchone()
    
    #print(rows)
    #print(row[0])
    cursor.commit()