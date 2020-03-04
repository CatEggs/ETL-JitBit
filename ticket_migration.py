# -*- coding: utf-8 -*-
import pandas as pd
import json
import requests
import config
from attachfiles import url_response
import jb_mapping as jmap

# def string_escape(s, encoding='utf-8'):
#     return (s.encode('latin1')         # To bytes, required by 'unicode-escape'
#              .decode('unicode-escape') # Perform the actual octal-escaping decode
#              .encode('latin1')         # 1:1 mapping back to bytes
#              .decode(encoding))

def main():
    jb_auth = (config.jb_username, config.jb_password)
    fd_auth = (config.fd_api_key, config.password)
    df = pd.read_excel(r'C:\Users\cegboh\Desktop\PythonProjects\TicketProject\\FD_MigratedTickets.xlsx')
    #df = pd.read_excel(r'C:\Users\cegboh\Desktop\PythonProjects\TicketProject\\FD_Tickets.xlsx')
    field_path = r"C:\Users\cegboh\Desktop\PythonProjects\TicketProject\\FD_TicketFields.txt"
    ticket_list = list(df['Ticket ID'])
    error_file = "log_file\\logfile.txt"
    error_file2 = "log_file\\missed_ticketlist.txt"
    completed_ticket = "log_file\\completedticketlist.txt"
    # update this to create a log file based on the date its run
    

    for id in ticket_list:
        r = requests.get('https://lienteam.freshdesk.com/api/v2/tickets/'+str(id)+'?include=conversations', auth = fd_auth)
    
        if r.status_code == 200:
            print("success")
            fd_response = json.loads(r.content)
            fd_ticketid = fd_response['id']
            try:
                fd_agent = fd_response['responder_id']
            except KeyError:
                fd_agent = None
            try:
                fd_requester = fd_response['requester_id']
            except KeyError:
                fd_requester = None
            fd_status = fd_response['status']
            fd_group = fd_response['group_id'] 
            fd_difficulty = fd_response['group_id'] #needs mapping
            try:
                fd_category = fd_response['custom_fields']['cf_detail']
            except KeyError:
                fd_category = "None"
            try:
                fd_section = fd_response['custom_fields']['cf_category']
            except KeyError:
                fd_section = "None"
            fd_plaintiff = fd_response['custom_fields']['cf_plaintiff_law_firm']
            fd_defendant = fd_response['custom_fields']['cf_defendant']
            fd_body = bytes(fd_response['description_text'], 'utf-8', errors="surrogateescape").decode('unicode_escape')
            fd_sub = fd_response['subject']
            fd_priority = fd_response['priority'] 
            fd_duedate = fd_response['due_by']
            fd_created = fd_response['created_at']
            try:
                fd_resolved = fd_response['stats']['resolved_at']
            except KeyError:
                fd_resolved = "None"
            field_file = open(field_path, "w", encoding="utf-8")
            field_file.write("Category: "+str(fd_section)+"\nSubcategory:"+str(fd_category)+"\nPlaintiff: "+str(fd_plaintiff)+"\nDefendant:"+str(fd_defendant)+"\nCreated Date: "+str(fd_created)+"\nResolved Date: "+str(fd_resolved)+ "\nBody: "+ str(fd_body))
            field_file.close()

            #headers = { 'Content-Type': 'multipart/form-data' }

            jb_payload = { 
            'categoryId' :jmap.categoryid_map[None], 
            'sectionId' : jmap.sectionid_map[None],
            'body' : fd_body,
            'subject' : fd_sub,
            'priorityId' : jmap.priority_map[fd_priority] ,
            'userId' : jmap.user_map.get(fd_requester)
            # possible to add attachement here
            }
            
            p = requests.post('https://'+ config.jb_url +'/helpdesk/api/ticket', auth = jb_auth , data = jb_payload)
            
            if p.status_code == 200:
                print("success, ticket w/ attachment created")
                jb_ticketid = json.loads(p.content)
                created_file = open(completed_ticket, "a")
                created_file.write(str(jb_ticketid) + " : "+ str(id) +"\n")
                attachment_data = {}
                for i in range(len(fd_response['attachments'])):
                    url = fd_response['attachments'][i]['attachment_url']
                    filename ='Orig' +'-' +str(i)+'-'+str(id) + '-'+ fd_response['attachments'][i]['name']
                    attachment_url = url_response(url, filename)
                    #attachment_url = s3_filetransfer(url, filename, jb_ticketid)
                    attachment_data.update({filename:open(attachment_url,'rb') })
                attach2_payload = {'id':jb_ticketid}
                linked_payload = {'ticketId':jb_ticketid, 'fieldId': "32410", 'value': str(id)}
                casename_payload = {'ticketId':jb_ticketid, 'fieldId': "32377", 'value': "Other"}
                agency_payload = {'ticketId':jb_ticketid, 'fieldId': "32378", 'value': "NA"}
                difficulty_payload = {'ticketId':jb_ticketid, 'fieldId': "32379", 'value': "Easy"}
                processtime_payload = {'ticketId':jb_ticketid, 'fieldId': "32373", 'value': "Less than 1 hour"}
                techteam_payload ={'ticketId':jb_ticketid, 'fieldId': "32596", 'value': "Data Team"}
                requests.post('https://'+ config.jb_url +'/helpdesk/api/AttachFile', auth = jb_auth , data = attach2_payload , files= {'file':open(field_path, 'rb')}) 
                requests.post('https://'+ config.jb_url +'/helpdesk/api/SetCustomField', auth = jb_auth , data = linked_payload)
                requests.post('https://'+ config.jb_url +'/helpdesk/api/SetCustomField', auth = jb_auth , data = casename_payload) 
                requests.post('https://'+ config.jb_url +'/helpdesk/api/SetCustomField', auth = jb_auth , data = agency_payload) 
                requests.post('https://'+ config.jb_url +'/helpdesk/api/SetCustomField', auth = jb_auth , data = difficulty_payload) 
                requests.post('https://'+ config.jb_url +'/helpdesk/api/SetCustomField', auth = jb_auth , data = processtime_payload)  
                requests.post('https://'+ config.jb_url +'/helpdesk/api/SetCustomField', auth = jb_auth , data = techteam_payload) 
                if bool(attachment_data):
                    attach2 = requests.post('https://'+ config.jb_url +'/helpdesk/api/AttachFile', auth = jb_auth , data = attach2_payload , files= attachment_data)
                    if attach2.status_code == 200:
                        print("Great")
                    else:
                        print("Status code:" +str(attach2.status_code)+". Error message:"+ attach2.text)
                else:
                    pass
                update_payload = {
                'id': jb_ticketid,
                'date': fd_created,
                'assignedUserId': jmap.user_map.get(fd_agent),
                'statusId': 3
                    }
                print(jb_ticketid)
                if fd_response['conversations']:
                    for i in range(len(fd_response['conversations'])):
                        conversation_user = fd_response['conversations'][i]['user_id']
                        conversation_body = fd_response['conversations'][i]['body_text']+'\n-'+ jmap.comment_userid.get(conversation_user)
                        conversation_data = {}
                        if fd_response['conversations'][i]['attachments']:
                            for j in range(len(fd_response['conversations'][i]['attachments'])):
                                if fd_response['conversations'][i]['attachments'][j]:
                                    filename = 'Conversation' +'-' +str(j) + '-'+str(id)+'-'+ fd_response['conversations'][i]['attachments'][j]['name']
                                    filepath =  url_response(fd_response['conversations'][i]['attachments'][j]['attachment_url'], filename)
                                    conversation_data.update({'file': open(filepath,'rb')})
                        conversation_payload = {
                        'id': jb_ticketid,
                        'body': conversation_body
                        }
                        attach_payload ={
                            'id':jb_ticketid
                        }
                        comment = requests.post('https://'+ config.jb_url +'/helpdesk/api/comment', auth = jb_auth , data = conversation_payload)
                        print('conversation has comment only')
                        if bool(conversation_data):
                            attach = requests.post('https://'+ config.jb_url +'/helpdesk/api/AttachFile', auth = jb_auth , data = attach_payload ,  files= conversation_data)
                            print('conversation has attachment')
                            if attach.status_code ==200:
                                print("FINALLLLLLLYYY")
                            else:
                                print(attach.text)
                                print("Attachement did not update. Status Code:"+str(attach.status_code))
                        else:
                            pass

                        if comment.status_code == 200:
                            print("comment successfully added")
                        else:
                            comment_error = (f"comment failed to update for ticketid:"+str(jb_ticketid)+".\n Status code: "+str(comment.status_code)+ "\n")
                            logg_file = open(error_file, "a")
                            logg_file.write(comment_error)
                            print(comment_error)
                u = requests.post('https://'+ config.jb_url +'/helpdesk/api/UpdateTicket?', auth = jb_auth , data = update_payload)
                if u.status_code == 200:
                    print("success, ticket successfully updated")
                    print(u.content)
                else:
                    update_error=("Ticket failed to update for Id:"+str(jb_ticketid)+"\n Status code:" +str(u.status_code)+"\n")
                    logg_file = open(error_file, "a")
                    logg_file.write(update_error)
                    print(u.content)
            else:
                attach_error = ("Attachment failed to update for ticketid: "+str(fd_ticketid)+".\n Status code:"+str(p.status_code))
                logg_file = open(error_file, "a")
                logg_file.write(attach_error)
                print(p.content)
                print(attach_error)
        else:
            jbticket_error = ("ticket failed to be created for ticketid:"+str(id)+".\n Status code: "+str(r.status_code)+"\n")
            logg_file = open(error_file, "a")
            logg_file.write(jbticket_error)
            logg_file2 = open(error_file2, "a")
            logg_file.write(str(id))
            print(r.content)
            print(jbticket_error)
main()

