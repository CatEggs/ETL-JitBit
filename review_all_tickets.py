import config
import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import date, datetime, timedelta
from dateutil.parser import parse
import pyodbc
import pandas as pd
import time

def get_fields(tix_response):
    try:
        priority_map = {
            -1 : 'Low', 0 : 'Normal',
            1 : 'High', 2 : 'Critical'
            }
        
        # get normal ticket fields from API response

        ticketid = str(tix_response['TicketID'])
        priority = int(tix_response['Priority'])
        createdate = str(tix_response['IssueDate'])
        subject = str(tix_response['Subject'])
        status = str(tix_response['Status'])
        #categoryid = tix_response['CategoryID']
        custusername = str(tix_response['SubmitterUserInfo']['FullName'])
        duedate = str(tix_response['DueDate'])
        lastupdate = str(tix_response['LastUpdated'])
        custdept = str(tix_response['SubmitterUserInfo']['DepartmentName'])
        tags = []
        tag_list = [tags.append(tix_response['Tags'][i]['Name']) for i in range(len(tix_response['Tags'])) if len(tix_response['Tags'][i]) > 0]
        try:
            tag = ";".join(tags)
        except TypeError:
            tag = None
        try:
            assignedto = str(tix_response['AssigneeUserInfo']['FullName'])
        except TypeError:
            assignedto = None
        try:
            assignedtodept = str(tix_response['AssigneeUserInfo']['DepartmentName'])
        except TypeError:
            assignedtodept = None
        resolvedate = str(tix_response['ResolvedDate'])
        categoryname = str(tix_response['CategoryName'])
        try:
            category, detail, = categoryname.split("/", 1)
        except ValueError:
            category, detail = categoryname, None

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
        
        
        nf_dict = {
            "ticketid":ticketid, "assignedtodept":assignedtodept, "assignedto":assignedto, "createdate":createdate, "status":status,
            "duedate_1":duedate_1,	"resolvedate_1": resolvedate_1,	"custdept":custdept,	"custusername":custusername, "category":category,
            "detail":detail,	"subject":subject, "lastupdate":lastupdate, "tag":tag
                }
        return nf_dict
    except Exception as e:
        return ("Failed to get normal field for TicketId: {0} : Error:{1} \n".format( str(ticketid),str(e)))

                    
def get_customfields(ticketid,customfield_response):
    try:
        
        arch_priority = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Archer Priority']
        casename = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Case Name']
        agency_col = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Agency/Collector Associated with Task']
        req_size = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Request Size']
        arch_status = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Archer Status']
        linked_id = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Linked Ticket Number']
        ticket_diff = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Ticket Difficulty']
        process_time = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Processing Time']

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
            linked_id = str(linked_id[0])
        except IndexError:
            linked_id = None
        try:
            ticket_diff = str(ticket_diff[0])
        except IndexError:
            ticket_diff = None
        try:
            process_time = str(process_time[0])
        except IndexError:
            process_time = None

        cf_dict = {
            "ticketid": ticketid, "arch_priority":arch_priority, "casename": casename, "agency_col":agency_col, "req_size":req_size,
                "arch_status":arch_status, "linked_id":linked_id, "ticket_diff":ticket_diff, "process_time":process_time
                }
        return cf_dict
    except Exception as e:
        return ("Failed to get custom field for TicketId: {0} : Error:{1} \n".format( str(id),str(e)))

def update_sql(ticketid, nf_dict, cf_dict):
    try:
        if nf_dict["ticketid"] == cf_dict["ticketid"] & nf_dict["ticketid"] == str(ticketid) :
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

            sql_insert = ('INSERT INTO JitBit (TicketId, TechTeam,	Technician,	CreateDate,	Status,	DueDate,	ResolveDate,	RequesterTeam,	Requester,	Casename,	Agency_Collector,	Category,	Detail,	Subject,	DifficultyLevel,	ProcessTime,	Archer_Priority,	LinkedTicket, UpdateDate, Tags) values (?, ?, ?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?, ?, ?)')
            cursor.execute(sql_insert, (nf_dict['ticketid'],	nf_dict['assignedtodept'],	nf_dict['assignedto'],	nf_dict['createdate'],	nf_dict['status'],	nf_dict['duedate_1'],	nf_dict['resolvedate_1'],	nf_dict['custdept'],	nf_dict['custusername'],	cf_dict['casename'],	cf_dict['agency_col'],	nf_dict['category'], nf_dict['detail'],	nf_dict['subject'],	cf_dict['ticket_diff'],	cf_dict['process_time'],	cf_dict['arch_priority'],	cf_dict['linked_id'], nf_dict['lastupdate'], nf_dict['tag']))
            sql_dedup = ('exec JitBit_DeDup')
            cursor.execute(sql_dedup)
            cursor.commit()
            
            return ("Done. JitBit Updated for {0}".format(str(nf_dict['ticketid'])))
        else:
            return ("TicketId mismatch for {0} & {1}".format(str(nf_dict['ticketid']), str(cf_dict['ticketid'])))
    except Exception as e:
        return ("TicketId: {0} : Error:{1} \n".format( str(ticketid),str(e)))

def check_status(response, filename, ticketid):

    logg_file = open(filename, "a+")
    if response.status_code == 200:
        return 1
    elif response.status_code == 429:
        print("429 for ticketid {0}. Header: {1}".format(str(ticketid),  response.headers))
        time.sleep(3)
        logg_file.write("Error code - {0} for TicketId:{1}\n".format(str(response.status_code),str(ticketid)))
        return 2
    else:
        logg_file.write("Error code - {0} for TicketId:{1}\n{2}\n".format(str(response.status_code),str(ticketid), response.header))
        return 0

def main():
    filename ="full_sweep_log\\allticket_log-2020_04_05.txt"
    logg_file = open(filename, "a+")
    # start_time creates a timestamp of when the script was executed. It will be written in a file called execute.py at the end of this script

    df = pd.read_excel(r"C:\Users\cegboh\Desktop\PythonProjects\TicketProject\\all_tickets.xlsx")

    # grabs only the ticketid from the json response and places it in a list.
    id_list = df['TicketID']

    for id in id_list:
        # try:
        r_nf = requests.get("https://"+ config.jb_url +"/helpdesk/api/Ticket?id="+str(id),auth=HTTPBasicAuth(config.jb_username, config.jb_password))
        time.sleep(1)
        r_cf = requests.get("https://"+config.jb_url+"/helpdesk/api/TicketCustomFields?id="+str(id),auth=HTTPBasicAuth(config.jb_username, config.jb_password))
        time.sleep(1)
        nf_statuscheck = check_status(r_nf,filename, id)
        cf_statuscheck = check_status(r_cf,filename, id)
        if nf_statuscheck == 1 or nf_statuscheck == 2:
            #print("API request for normal status check's good for ticketid:{0}".format(str(id)))
            tix_response = json.loads(r_nf.content)
            ticket_fields = get_fields(tix_response)
        else:
            print("nf_statuscheck failed and passed")
            pass
        if cf_statuscheck == 1 or cf_statuscheck == 2:
            #print("API request for custom status check's good for ticketid:{0}".format(str(id)))
            customfield_response = json.loads(r_cf.content)
            ticket_custfields = get_customfields(id,customfield_response)
            if isinstance(ticket_fields, dict) and isinstance(ticket_custfields, dict):
                update_sql(id, ticket_fields, ticket_custfields)
                print("Done! Ticket Updated for ticketid {0}".format(str(id)))
            else: 
                logg_file.write("DictionaryCheck is False.\n" + ticket_fields +" - "+ ticket_custfields + "\n")
                pass
        else:
            print("cf_statuscheck failed and passed")
            pass
#         except Exception as e:
#             #logg_file.write("Code broke for ticketid {0}. Error: {1}\n".format(str(id),str(e)))
#             print("Code broke for ticketid {0}. Error: {1}".format(str(id),str(e)))
main()
            