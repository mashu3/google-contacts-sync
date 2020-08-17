import csv
import time
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

CONTACT_CSV_FILE = 'smaple.csv'
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/contacts']
CLIENT_SECRET_FILE = 'credentials.json'
MAX_CONTACTS_SIZE = 10000

class ImportContacts(object):
    def read_contacts(self, file_name):
        # Import the csv file of the address book and convert it into a list
        with open(file_name, 'r') as f:
            csv_reader   = csv.reader(f)
            csv_header   = next(csv_reader) # Skip the header line
            csv_contacts = []
            for csv_contact in csv_reader:
                csv_contacts.append(csv_contact)
        return csv_contacts
    
    def print_all_contacts(self, file_name):
        csv_contacts = self.read_contacts(file_name)
        for i, csv_contact in enumerate(csv_contacts):
            print('no.%s %s' % (i+1, csv_contact))

class QuickstartMod(object):
    def __init__(self):
        # local_list is a list of IDs of records to be uploaded.
        # Initialized with a negative number, meaning that this element number contains no data.
        self.local_list = [-1]*MAX_CONTACTS_SIZE

        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow  = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        creds = creds
        self.service = build('people', 'v1', credentials=creds)

    def get_all_contacts(self):
        # Keep getting 1000 connections until the nextPageToken becomes None
        connections_list = []
        next_page_token  = ''
        while True:
            if not (next_page_token is None):
                # Call the People API
                results = self.service.people().connections().list(
                        resourceName = 'people/me',
                        pageSize     = 1000,
                        personFields = 'names,userDefined',
                        pageToken    = next_page_token
                        ).execute()
                connections_list = connections_list + results.get('connections', [])
                next_page_token  = results.get('nextPageToken')
            else:
                break
        return connections_list

    def print_all_contacts(self):
        connections_list = self.get_all_contacts()
        if connections_list:
            for i, person in enumerate(connections_list):
                resource_name = person.get('resourceName', [])
                names = person.get('names', [])
                if names:
                    display_name = names[0].get('displayName')
                else:
                    display_name = None
                print('no.%s %s %s' % (i+1, display_name, resource_name))
        else:
            print('No connections found.')

    def create_contact_body(self, contact):
        given_name   = contact[1]
        middle_name  = contact[2]
        family_name  = contact[3]
        notes        = contact[25]
        email_work   = contact[30]
        email_home   = contact[32]
        phone_mobile = contact[34]
        phone_work   = contact[36]
        phone_home   = contact[38]
        address_home = contact[40] # The unstructured value of the address
        p_code_home  = contact[45]
        address_work = contact[50] # The unstructured value of the address
        p_code_work  = contact[55]
        org_name     = contact[58]
        org_title    = contact[60]
        department   = contact[61]
        record_id    = contact[65] # the user defined data, intended to be used for synchronization
        modified_day = contact[66] # the user defined data, intended to be used for synchronization

        contact_body = {
            'names':[{'givenName':given_name, 'familyName':family_name, 'middleName':middle_name,}],
            'emailAddresses':[{'value':email_work, 'type':'work'}, {'value':email_home, 'type':'home'}],
            'phoneNumbers':[{'value':phone_mobile, 'type':'mobile'},
                            {'value':phone_home, 'type':'home'},
                            {'value':phone_work, 'type':'work'}],
            'addresses':[{'streetAddress':address_home, 'postalCode':p_code_home, 'type':'home'},
                         {'streetAddress':address_work, 'postalCode':p_code_work, 'type':'work'}],
            'organizations':[{'name':org_name, 'title':org_title}, {'department':department}],
            'biographies':[{'value':notes}],
            'userDefined':[{'key':record_id, 'value':modified_day}]
        }
        return contact_body

    # Upload contacts one by one.
    def add_contact(self, contact):
        try:
            contact_body = self.create_contact_body(contact)
            new_contact  = self.service.people().createContact(body=contact_body).execute()
            print('Upload id:%s %s %s' % (contact[-2], contact[1], contact[3]))
        except IndexError:
            print('Error id:%s %s %s contains input errors' % (contact[-2], contact[1], contact[3]))
            pass
    
    def delete_contact(self, entry):
        if not entry.get('displayName') is None:
            self.service.people().deleteContact(resourceName=entry.get('resourceName')).execute()
            print('Delete id:%s %s' % (entry.get('record_id'), entry.get('displayName')))
        else :
            self.service.people().deleteContact(resourceName=entry.get('resourceName')).execute()
            print('Delete id:%s' % (entry.get('record_id')))

    def create_contact_group(self, group_name):
        results  = self.service.contactGroups().create(body={'contactGroup':{'name':group_name}}).execute()
        group_id = results.get('resourceName', [])
        return group_id

    def delete_all_contacts(self):
        group_id = self.create_contact_group('temp')
        print('making a temporary group to delete...')
        time.sleep(5)
        print('waiting ...')
        time.sleep(5)
        connections = self.get_all_contacts()
        if connections:
            for i, person in enumerate(connections):
                resource_name = person.get('resourceName', [])
                add_group = self.service.contactGroups().members().modify(resourceName=group_id, body={'resourceNamesToAdd':[resource_name]}).execute()
                print(resource_name +' moved to the temporary group')
        else:
            print('No connections found.')
        self.service.contactGroups().delete(resourceName=group_id, deleteContacts=True).execute()
        print('All contacts have been deleted.')

    def sync_contacts(self, contacts):
        # Storing csv information in local_list
        for i, contact in enumerate(contacts):
            if contact[-2].isdigit():
                self.local_list[int(contact[-2])] = i # Put the id in the address book csv

        # In GoogleContacts, there are fields that allows users to set up their own data.
        # The following information is added to userDefined.
        # userDefined['key']   = the id in the address csv
        # userDefined['value'] = modification date
        # Based on these, the data to be uploaded is determined.
        # id exists only in local  => upload
        # id exists only in google => delete
        # id exists in both and the modification date is newer than local => Update (delete and upload)
        connections = self.get_all_contacts()
        if connections:
            for i, person in enumerate(connections):
                resource_name = person.get('resourceName', [])
                names = person.get('names', [])
                if names:
                    display_name = names[0].get('displayName')
                else:
                    display_name = None
                userDefined = person.get('userDefined', [])
                if userDefined:
                    record_id    = userDefined[0].get('key')
                    modified_day = userDefined[0].get('value')
                    try:
                        idbuf = int(record_id)
                    except IndexError:
                        print('strange data is in gmail. delete it now.')
                        self.service.people().deleteContact(resourceName=resource_name).execute()
                        continue
                else:
                    record_id    = None
                    modified_day = None

                entry = dict(resourceName=resource_name, displayName=display_name, record_id=record_id, modified_day=modified_day)
                
                if self.local_list[idbuf] < 0:
                # If local_list[idbuf] is -1, it means that there is no data in the csv of the address book.
                    self.delete_contact(entry)
                else:
                # If the update date is new, it will be updated.
                    try:
                        if contacts[self.local_list[idbuf]][-1].count(":") == 1:
                            contacts[self.local_list[idbuf]][-1] = contacts[self.local_list[idbuf]][-1] + ":00"
                        buftime = modified_day
                        if buftime.count(":") == 1:
                            buftime = buftime + ":00"
                        if time.strptime(buftime,"%Y/%m/%d %H:%M:%S") < time.strptime(contacts[self.local_list[idbuf]][-1],"%Y/%m/%d %H:%M:%S"):
                            self.delete_contact(entry)
                            self.add_contact(contacts[self.local_list[idbuf]])
                    except ValueError:
                        print('ValueError at id:', record_id)
                        self.delete_contact(entry)
                        self.add_contact(contacts[self.local_list[idbuf]])
                    # Check completion flag
                    self.local_list[idbuf] = -1

        # Upload the rest of the list
        for flag in self.local_list:
            if flag >= 0:
                self.add_contact(contacts[flag])

def main():
    read_contacts = ImportContacts()
    contacts_list = read_contacts.read_contacts(CONTACT_CSV_FILE)
    #read_contacts.print_all_contacts(CONTACT_CSV_FILE)
    sync_contacts = QuickstartMod()
    #data = []
    #for contact in contacts_list:
    #    data.append(sync_contacts.add_contact(contact))
    print('start updating google contacts ...')
    sync_contacts.sync_contacts(contacts_list)
    print('update completed')
    #sync_contacts.delete_all_contacts()

if __name__ == '__main__':
    main()