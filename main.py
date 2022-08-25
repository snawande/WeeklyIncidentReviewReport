from venv import create
import requests
import pandas as pd
from datetime import datetime
import json
from collections import OrderedDict
import warnings
import openpyxl
warnings.filterwarnings("ignore")

# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', -1)

headers = {'Authorization': 'Basic c3ZjUGFhU1Byb3Zpc2lvbmluZ1NlcnZpY2U6M3FMUSQ0YkNRIw=='}


def get_user_name(api_req_link):
    response = requests.get(api_req_link, headers=headers, verify=False)
    data = json.loads(response.text)
    name = data['result']['name']
    return name


def get_parent(api_req_link):
    response = requests.get(api_req_link, headers=headers, verify=False)
    data = json.loads(response.text)
    api_req_link = data['result']['department']['link']

    count = 0
    while (count < 5):
        response = requests.get(api_req_link, headers=headers, verify=False)
        data = json.loads(response.text)
        api_req_link = data['result']['parent']['link']
        count += 1
    return data['result']['description']

def get_other_parent(api_req_link):
    response = requests.get(api_req_link, headers=headers, verify=False)
    data = json.loads(response.text)
    api_req_link = data['result']['u_business_unit']['link']

    response = requests.get(api_req_link, headers=headers, verify=False)
    data = json.loads(response.text)
    return data['result']['description']

def get_records(complete_url, tag_name):
    response = requests.get(complete_url, headers=headers, verify=False)
    data = json.loads(response.text)
    sum = 0
    full_data = []
    state_dict = {"1" : "New",
                "2" : "In Queue",
                "3" : "Assigned and In Progress",
                "4" : "Pending",
                "5" : "Closed",
                "6" : "Resolved"}

    for item in data['result']:
        # incident number
        inc_num = item['number']

        # pending, assigned and in progress, or resolved
        try:
            state = state_dict[item['state']]
        except:
            state = 'Other State'

        #date opened at
        opened_at = item['opened_at']
        
        # assignment group
        try:
            assignment_group = get_user_name(item["assignment_group"]["link"])
        except:
            assignment_group = ''

        #priority
        priority = item["priority"]
        
        #opened by
        opened_by = get_user_name(item["caller_id"]["link"])
        
        #parent
        try:
            parent = get_parent(item["requested_by"]["link"])
        except:
            parent = "(empty)"

        #other parent
        try:
            other_parent = get_other_parent(item["requested_by"]["link"])
        except:
            other_parent = "('empty')"

        try:
            if item["cmdb_ci"] == "":
                cmdb_ci_name = "(empty)"
            elif item["cmdb_ci"]["value"] == "e58c1f73db871c1090465a98dc96190c":
                cmdb_ci_name = "SaaS"
            else:
                cmdb_ci_name = get_user_name(item["cmdb_ci"]["link"])
        except:
            cmdb_ci_name = "(empty)"

        arr = [inc_num, tag_name, state, assignment_group, priority, cmdb_ci_name, opened_by, other_parent, parent]
        full_data.append(arr)
        sum += 1
    return full_data


def get_first_pivot_df(df):
    # segment and status dataframe
    first_pivot_data = []
    unique_segments = (list(df.Segment.unique()))
    State_values = (list(df.State.unique()))
    for segment in unique_segments:
        data = []
        for state in State_values:
            val = len(df[(df['Segment'] == segment) & (df['State'] == state)])
            data.append(val)
        first_pivot_data.append(data)

    first_pivot_df = pd.DataFrame(first_pivot_data, columns = State_values, index = unique_segments)
    return first_pivot_df

def get_second_pivot_df(df):
    # Status Resolved and tagging
    first_pivot_data = []
    unique_segments = (list(df.Segment.unique()))
    State_values = (list(df.State.unique()))
    for segment in unique_segments:
        data = []
        for state in State_values:
            val = len(df[(df['Segment'] == segment) & (df['State'] == state)])
            data.append(val)
        first_pivot_data.append(data)

    first_pivot_df = pd.DataFrame(first_pivot_data, columns = State_values, index = unique_segments)
    return first_pivot_df

def get_third_pivot_df(df):
    #nan for empty tags
    unique_tags = ['(empty)', 'DPSE - App', 'DPSE - Platform', 'DPSE - Consult', 'DPSE - EP3']
    unique_teams = (list(df['Assignment Group'].unique()))
    unique_priorities = (list(df['Priority'].unique()))
    total_data_dict = {}
    col_data = []

    for priority in unique_priorities:
        total_data_dict[str(priority)] = []

    # print(total_data_dict)
    
    for team in unique_teams:
        for tag in unique_tags:
            if tag == '(empty)':
                col_data.append(team + '(no tags)')
            else:
                col_data.append(tag)
            
            for priority in unique_priorities:
                val = len(df[(df['Assignment Group'] == team) & (df['Tags'] == tag) & (df['Priority'] == priority)])
                total_data_dict[str(priority)].append(val)

    third_pivot_df = pd.DataFrame(total_data_dict, index = col_data)


    third_pivot_df.rename(columns={'4' : '4 - Low', 
                                   '3' : '3 - Moderate',
                                   '2' : '2 - High',
                                   '1' : '1 - Critical'}, inplace = True)
    
    return third_pivot_df

def get_second_pivot_df(df, state):
    second_pivot_data = []
    unique_tags = ['DPSE - App', 'DPSE - Platform', 'DPSE - Consult', 'DPSE - EP3']
    for tag in unique_tags:
        val = len(df[(df['State'] == state) & (df['Tags'] == tag)])
        second_pivot_data.append(val)
    second_pivot_df = pd.DataFrame(second_pivot_data, columns = [state], index = unique_tags)

    # add percentage column
    total = second_pivot_df[state].sum()
    percentage_arr = []
    if total == 0:
        percentage_arr = [0, 0, 0, 0]
    else:
        for item in second_pivot_df[state]:
            percentage_arr.append(str(item/total * 100) + '%')
    second_pivot_df['Percentage'] = percentage_arr
    second_pivot_df.loc["Total"] = second_pivot_df.sum()
    second_pivot_df.at['Total','Percentage'] = '100%'
    return second_pivot_df

#filter for tags

basic_url = 'https://manulife.service-now.com/api/now/table/incident?sysparm_limit=1000&sparm_display_value=true&'
query = 'sysparm_query=assignment_group.name{}LIKEDevplat-Rel'

sys_tags_dpse_app = '{}sys_tags.75b35c1fdbf894506d834d8b139619a9=75b35c1fdbf894506d834d8b139619a9'
sys_tags_dpse_platform = '{}sys_tags.8139ed4adb2498101ba5ee0c1396191d=8139ed4adb2498101ba5ee0c1396191d'
sys_tags_dpse_consult = '{}sys_tags.780283751bcdf0105d430d42604bcb96=780283751bcdf0105d430d42604bcb96'
sys_tags_dpse_ep3 = '{}sys_tags.47b183351bcdf0105d430d42604bcbb8=47b183351bcdf0105d430d42604bcbb8'
timeline = '^sys_created_onBETWEENjavascript:gs.daysAgoStart(7)@javascript:gs.endOfToday()'


app_tags_url = basic_url + query.format('') + sys_tags_dpse_app.format('^') + timeline
platform_tags_url = basic_url + query.format('') + sys_tags_dpse_platform.format('^') + timeline
consult_tags_url = basic_url + query.format('') + sys_tags_dpse_consult.format('^') + timeline
ep3_tags_url = basic_url + query.format('') + sys_tags_dpse_ep3.format('^') + timeline
NOTapp_tags_url = basic_url + query.format('NOT') + sys_tags_dpse_app.format('^') + timeline
NOTplatform_tags_url = basic_url + query.format('NOT') + sys_tags_dpse_platform.format('^') + timeline
NOTconsult_tags_url = basic_url + query.format('NOT') + sys_tags_dpse_consult.format('^') + timeline
NOTep3_tags_url = basic_url + query.format('NOT') + sys_tags_dpse_ep3.format('^') + timeline
my_complete_url = basic_url + query.format('') + sys_tags_dpse_app.format('^OR') + sys_tags_dpse_platform.format('^OR') + sys_tags_dpse_consult.format('^OR') + sys_tags_dpse_ep3.format('^OR') + timeline


app_tags_url_record = get_records(app_tags_url, 'DPSE - App')
platform_tags_url_record = get_records(platform_tags_url, 'DPSE - Platform')
consult_tags_url_record = get_records(consult_tags_url, 'DPSE - Consult')
ep3_tags_url_record = get_records(ep3_tags_url, 'DPSE - EP3')
NOTapp_tags_url_record = get_records(NOTapp_tags_url, 'DPSE - App')
NOTplatform_tags_url_record = get_records(NOTplatform_tags_url, 'DPSE - Platform')
NOTconsult_tags_url_record = get_records(NOTconsult_tags_url, 'DPSE - Consult')
NOTep3_tags_url_record = get_records(NOTep3_tags_url, 'DPSE - EP3')
complete_url_record = get_records(my_complete_url, '(empty)')

full_data = app_tags_url_record + platform_tags_url_record + consult_tags_url_record + ep3_tags_url_record
NOT_full_data = NOTapp_tags_url_record + NOTplatform_tags_url_record + NOTconsult_tags_url_record + NOTep3_tags_url_record
mega_lst = full_data + NOT_full_data

# remove dups
mega_lst = list(set(map(lambda i: tuple(i), mega_lst)))

new_mega_lst = []
for elem in mega_lst:
    new_mega_lst.append(list(elem))
for i in range(len(complete_url_record)):    
    for j in range(len(new_mega_lst)):
        # found a match -> complete record copy tag
        if complete_url_record[i][0] == new_mega_lst[j][0]:
            if complete_url_record[i][1] == new_mega_lst[j][1]:
                complete_url_record[i][1] += new_mega_lst[j][1]
            else:
                complete_url_record[i][1] = new_mega_lst[j][1]

columns = ['Incident Number', 'Tags', 'State' ,'Assignment Group', 'Priority', 'Business Service/CI', 'Opened By', 'Parent', 'Second Parent']
df = pd.DataFrame(complete_url_record, columns = columns)
# df.to_excel('result.xlsx', sheet_name = 'Data', index = False)

# add segment column
lookup_df = pd.read_excel('Segment_Lookup.xlsx')
Parent_Arr = lookup_df['Parent'].to_list()
Second_Parent_Arr = lookup_df['Second Parent'].to_list()
Segment_Arr = lookup_df['Segment'].to_list()
lookup_dict = {}

for i in range(len(Parent_Arr)):
    lookup_dict.update({(Parent_Arr[i], Second_Parent_Arr[i]) : Segment_Arr[i]})

lookup_df = pd.read_excel('Segment_Lookup.xlsx')
Parent_Arr = lookup_df['Parent'].to_list()
Second_Parent_Arr = lookup_df['Second Parent'].to_list()
Segment_Arr = lookup_df['Segment'].to_list()

lookup_dict = {}
for i in range(len(Parent_Arr)):
    lookup_dict.update({(Parent_Arr[i], Second_Parent_Arr[i]) : Segment_Arr[i]})
tup_list = []
df_parent = df['Parent'].to_list()
df_second_parent = df['Second Parent'].to_list()
for i in range(len(df_parent)):
    tup_list.append((df_parent[i],df_second_parent[i]))

segment_col = []
for item in tup_list: 
    segment_col.append(lookup_dict[item])

df['Segment'] = segment_col
first_pivot_df = get_first_pivot_df(df)

assigned_in_prog_tags_df = get_second_pivot_df(df, 'Assigned and In Progress')
pending_tags_df = get_second_pivot_df(df, 'Pending')
closed_tags_df = get_second_pivot_df(df, 'Closed')
in_queue_tags_df = get_second_pivot_df(df, 'In Queue')
third_df = get_third_pivot_df(df)

with pd.ExcelWriter("report.xlsx") as writer:
    df.to_excel(writer, sheet_name = 'Full Data', index = False)
    first_pivot_df.to_excel(writer, sheet_name = 'Segment Data')
    assigned_in_prog_tags_df.to_excel(writer, sheet_name = 'Assigned and In Progress')
    pending_tags_df.to_excel(writer, sheet_name = 'Pending')
    closed_tags_df.to_excel(writer, sheet_name = 'Closed')
    in_queue_tags_df.to_excel(writer, sheet_name = 'In Queue')
    third_df.to_excel(writer, sheet_name = 'Summary')

