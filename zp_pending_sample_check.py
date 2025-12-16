import pandas as pd
import numpy as np
import cx_Oracle
from datetime import date
import csv
import os

#Function to read the profile file.
def readProfile(vars, path):
    values = {}
    with open(path, "r") as file:
        for line in file:
            line=line.strip()
            if line and not line.startswith('#'):
                for var in vars:
                    if line.startswith(f"{var}="):
                        value=line.split("=",1)[1].strip('"').strip('"')
                        values[var]=value
    return values

def unix(command):
    try:
        exit_code = os.system(command)
        if exit_code != 0:
            raise Exception(f"Unix shell script failed with exit code {exit_code}")
    except Exception as e:
        print(f"Error: {e}")

def styledTable(df):
    table_html = df.to_html(index=False, escape=False)
    
    #Style the header    
    styled_table = table_html.replace(
        '<thead>',
        '<thead><tr style="background-color: #2b88d8; color: white;>'
    ).replace(
        '</thead>',
        '</tr></thead>'
    )

    #Style rows with alternate colors
    body = styled_table.split('<tbody>')[1].split('</tbody>')[0]
    rows = body.split('<tr>')[1:] #split rows.
    styled_rows = []

    for i, row_data in enumerate(df.values): #row_data is the list of cell values in a row
        bg_color = '#c7e0f4' if i % 2 == 0 else '#ffffff'

        #check the condition for row highlighting
        if row_data[-1] == 'file not found':
            bg_color = '#ffff99'

        styled_row = f'<tr style="background-color: {bg_color};">{rows[i]}'
        styled_rows.append(styled_row)

    styled_table=(
        styled_table.split('<tbody>')[0] + '<tbody>' + ''.join(styled_rows) + '</tbody></table>'
    )
    return styled_table

def queryDB(username, password, hostname, port, sid, query):

    try:
        os.environ["NLS_LANG"] = ".UTF8"
        dsn = cx_Oracle.makedsn(hostname, port, sid=sid)
        connection = cx_Oracle.connect(
            user=username,
            password=password,
            dsn=dsn,
            encoding="UTF-8",   #For standard character set
            nencoding="UTF-8"   #For national character set
        )
        # print('Connection successful')

    except cx_Oracle.DatabaseError as e:
        print("ERROR: ", e)

    try:
        cursor = connection.cursor()
        cursor.execute(query)

        columns = [col[0] for col in cursor.description]

        data = cursor.fetchall()
        # print('Query executed successfully\n')

        df_SF_ZP_Report = pd.DataFrame(data, columns=columns)
        
        return df_SF_ZP_Report

    except cx_Oracle.DatabaseError as e:
        print(f"Query execution failed: {e}")

    finally:
        cursor.close()
        connection.close()
        # print("Database connection closed.")

def findFileName(refNmbr, directory, columnName):
    fileFound = 0
    for fnm in os.listdir(directory):
        if fnm.endswith(".csv"):
            filepath = os.path.join(directory,fnm)
            try:
                with open(filepath, mode='r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile) # access columns by name

                    for row in reader:
                        if refNmbr.lower() in row[columnName].lower():
                            fileFound = 1
                            dict_filenames[refNmbr] = fnm
                            break
            except Exception as e:
                print(f"Error reading {fnm}: {e}")

        if fileFound==0:
            dict_filenames[refNmbr] = "file not found"
            dict_nfRefNbrs[refNmbr] = "file not found"

today = date.today()
todaysDate = today.strftime("%B %d, %Y")

#SQL query to extract the pending ZP samples details data.
query = """
    select s.msd_core_country_code as "Country Code", u.name as "Call: Owner Name", s.name as "Call Sample: Call Sample Name", s.id as "Call Sample: ID",
    s.CALL_DATE_VOD as "Datetime", acc.name as "Account: Name", pacc.name as "Account: Primary Parent", prod.name as "Product", s.quantity_vod as "Quantity", c.LAST_MODIFIED_DATE
    from vva_mirror_pub.pub_call2_sample s join vva_mirror_pub.pub_call2 c on s.CALL2_VOD = c.id
    join vva_mirror_pub.pub_user u on s.CREATEDBYID = u.id
    join vva_mirror_pub.pub_account acc on s.account_vod = acc.id
    join vva_mirror_pub.pub_account pacc on acc.primary_parent_vod = pacc.id
    join vva_mirror_pub.pub_product prod on s.product_vod = prod.id
    and s.MSD_CORE_COUNTRY_CODE IN ('HK','SG','MY','TH')
    and s.IS_PARENT_CALL_VOD = '1'
    and s.ISDELETED = 'N'
    AND s.DELIVERY_STATUS_VOD not in ('Cancelled','Cancelled_vod','Delivered_vod','Completed')
    and trunc(s.CALL_DATE_VOD) < to_date(to_char((sysdate-1),'dd/mm/yyyy')||'20:30:00','dd/mm/yyyy hh24:mi:ss')
    and c.STATUS_VOD IN ('Submitted', 'Submitted_vod')
    and trunc(c.LAST_MODIFIED_DATE) < to_date(to_char((sysdate-1),'dd/mm/yyyy')||'20:30:00','dd/mm/yyyy hh24:mi:ss')
    order by s.msd_core_country_code, s.CALL_DATE_VOD
    """

#Read the APAC DB credentials from the Profile file
vars = ["username","password","hostname","port","sid","zp_receiver_email","zp_cc_email","zp_nf_values_receiver_email"]
path = "/iics_pmroot/Profiles/gblsfa_ZP_AUTOMATION.profile"
apacCredentials = readProfile(vars, path)
username = apacCredentials.get("username", None)
password = apacCredentials.get("password", None)
hostname = apacCredentials.get("hostname", None)
port = apacCredentials.get("port", None)
sid = apacCredentials.get("sid", None)
zp_receiver_email = apacCredentials.get("zp_receiver_email", None)
zp_cc_email = apacCredentials.get("zp_cc_email", None)
zp_nf_values_receiver_email = apacCredentials.get("zp_nf_values_receiver_email", None)

# print("Connecting to APAC DB")
df_report = queryDB(username, password, hostname, port, sid, query)

rows = len(df_report.axes[0])

dict_filenames = dict() #for files found
dict_nfRefNbrs = dict() #for files not found

for i in range(rows):
    country = df_report.iloc[i]['Country Code']
    refNmbr = df_report.iloc[i]['Call Sample: ID']
    directory = f"/iics_pmroot/ARCHIVE/SMP_ZP_OUT_IICS/{country}/"

    csvColumnName = 'Customer PO#'
    findFileName(refNmbr, directory, csvColumnName)
    
# print(dict_filenames)
df_sampleList = pd.DataFrame(list(dict_filenames.items()), columns=['Call Sample: ID', 'ZP File Name'])

#Merge to df with on ID. Creates a new column at the end.
report = pd.merge(df_report,df_sampleList,how='inner',on='Call Sample: ID')

# #Rearrange the order the columns
finalReport = report[['Country Code','Call: Owner Name', 'Call Sample: Call Sample Name', 'Call Sample: ID', 'Datetime', 'Account: Name', 'Account: Primary Parent', 'Product',	'Quantity', 'ZP File Name']].copy()

#Pivot table
summary = pd.pivot_table(finalReport,index=['Country Code'] ,values='Call Sample: ID', aggfunc=np.count_nonzero).reset_index()
summary.rename(columns={'Country Code':'Market', 'Call Sample: ID': 'Count of sample calls'},inplace=True)

summary.loc[len(summary.index)] = ['Grand Total', summary['Count of sample calls'].sum()]

# print(summary)

sampleCount = summary.iloc[4]['Count of sample calls']

#For files not found.
if 'file not found' in finalReport['ZP File Name'].values:
    nf_count = finalReport['ZP File Name'].value_counts()['file not found']

    df_nfList = pd.DataFrame(list(dict_nfRefNbrs.items()), columns=['Call Sample: ID', 'ZP File Name'])
    nfReport = pd.merge(df_report,df_nfList,how='inner',on='Call Sample: ID')
    nfFinalReport = nfReport[['Country Code','Call: Owner Name', 'Call Sample: Call Sample Name', 'Call Sample: ID', 'Datetime', 'Account: Name', 'Account: Primary Parent', 'Product',	'Quantity', 'ZP File Name']].copy()

    nfReportTable = styledTable(nfFinalReport)

    nf_mail_body = f"""
    <html>
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, intial-scale=1.0">
        <style>
        body {{
            font-family: "Aptos", Arial, Helvetica, sans-serif;
            font-size: 14px;
            line-height: 1.5;
            margin: 0;
            padding: 0;
        }}
        table{{
            width: auto;
            border-collapse: collapse;
            table-layout: auto;
            font-size: 12px
        }}
        th, td{{
            border: 1px solid #2b88d8;
            text-align: left;
        }}
        tr{{
            line-height: 16px;
            padding: 5px;
        }}
        </style>
    </head>
    <body>
        <p>Hi Team,</p>
        <p></p>
        <p>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;For the ZP pending samples report of {todaysDate}, there are {sampleCount} call sample IDs found for which no files are present.
        </p>
        <p></p>
        <p>Report with files not found:</p>
        <div class="table">
        {nfReportTable}
        </div>
        <p></p>
        <p></p>
        <p style="margin-top: 30px;">Thanks and regards.</p>
    </body>
    </html>
    """

    #Save the mail body as .html file to fire email with linux command.
    nf_mail_body_file = "/iics_pmroot/Temp/zp_report_nf_body.html"
    with open(nf_mail_body_file, "w", encoding='utf-8') as file:
        file.writelines('<meta charset="UTF-8">\n')
        file.write(nf_mail_body)

    subject = f"Files not found for ZP pending samples report on {todaysDate}"

    sendMailCommand_nf = f"""
cat <<'EOF' - {nf_mail_body_file} | /usr/sbin/sendmail -t
To: {zp_nf_values_receiver_email}
Subject: {subject}
Content-Type: text/html
EOF
"""
    #send mail using unix.
    unix(sendMailCommand_nf)

else:
    nf_count = 0

# print('\nTotal Sample Calls Count: ',sampleCount)
# print('NF Count: ',nf_count)

summaryTable = styledTable(summary)
reportTable = styledTable(finalReport)

html_body_1 = f"""
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, intial-scale=1.0">
    <style>
        body {{
            font-family: "Aptos", Arial, Helvetica, sans-serif;
            font-size: 14px;
            line-height: 1.5;
            margin: 0;
            padding: 0;
        }}
        table{{
            width: auto;
            border-collapse: collapse;
            table-layout: auto;
            font-size: 12px
        }}
        th, td{{
            border: 1px solid #2b88d8;
            text-align: left;
        }}
        tr{{
            line-height: 16px;
            padding: 5px;
        }}
    </style>
</head>
<body>
    <p>Hi All,</p>
    <p></p>"""

html_body_3=f"""
    <p></p>
    <p>Summary:</p>
    <div class="table">
    {summaryTable}
    </div>
    <p></p>
    <p>Details:</p>
    <div class="table">
    {reportTable}
    </div>
    <p></p>
    <p></p>
    <p style="margin-top: 30px;">Thanks and regards.</p>
    <p style="margin-top: 30px;">Note: This is an Auto-Generated Email. Please reach out to cim_l2_support@organon.com for any queries or 
    <a href="https://support.organon.com/sp?id=sc_cat_item&sys_id=8e343afadb61fc503ed756086896195d&sysparm_category=7779452587c93c1449017597cebb3548&catalog_id=e0d08b13c3330100c8b837659bba8fb4">click here</a> to raise a ticket.</p>
</body>
</html>
"""

if(nf_count == 0):
    html_body_2 = f"""
        <p>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;As of {todaysDate}, we have {sampleCount} Pending calls and we have successfully transferred all to ZP in the files listed below.
        </p>
    """

elif(nf_count == 1):
    html_body_2 = f"""
        <p>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;As of {todaysDate}, we have {sampleCount} Pending calls and we have successfully transferred {sampleCount-nf_count} calls to ZP in the files listed below.
        </p>
        <p>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The {nf_count} highlighted call was not processed and team is investigating on this.
        </p>
    """

else:
    html_body_2 = f"""
        <p>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;As of {todaysDate}, we have {sampleCount} Pending calls and we have successfully transferred {sampleCount-nf_count} calls to ZP in the files listed below.
        </p>
        <p>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The {nf_count} highlighted calls were not processed and team is investigating on this.
        </p>
    """
mail_body = html_body_1 + html_body_2 + html_body_3

#Save the mail body as .html file to fire email with linux command.
mail_body_file = "/iics_pmroot/Temp/zp_report_mail_body.html"
with open(mail_body_file, "w", encoding='utf-8') as file:
    file.writelines('<meta charset="UTF-8">\n')
    file.write(mail_body)

subject = f"Daily Check for Pending Samples as of {todaysDate}"

sendMailCommand = f"""
cat <<'EOF' - {mail_body_file} | /usr/sbin/sendmail -t
To: {zp_receiver_email}
Cc: {zp_cc_email}
Subject: {subject}
Content-Type: text/html

EOF
"""

#send mail using unix
unix(sendMailCommand)