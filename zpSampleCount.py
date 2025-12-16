import pandas as pd #install in linux
import numpy as np  #install in linux
import cx_Oracle
from datetime import date
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
    os.system(command)
    # print(f"Unix command executed successfully - {command}")

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

# #APAC
# username = "RO_CE_CIM"
# password = "ECIAP#836"
# hostname = "napjcep1.crchsh35oml1.ap-southeast-1.rds.amazonaws.com"
# port = "25881"
# sid = "NAPJCEP1"

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

print('\nTotal Sample Calls Count: ',rows)

mail_body = f"""
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
    </style>

</head>
<body>
    <p>Hi,</p>
    <p></p>
    <p>Total number of pending sample extracted from DB = {rows}</p>
</body>
</html>
"""

#Save the mail body as .html file to fire email with linux command.
mail_body_file = "/iics_pmroot/Temp/test_zp_count_mail_body.html"
with open(mail_body_file, "w", encoding='utf-8') as file:
    file.writelines('<meta charset="UTF-8">\n')
    file.write(mail_body)

# receiver_email = "vivek.rugale@organon.com"
# cc_email = 'vivek.rugale@organon.com'
subject = f"Test - Count check for Pending Samples - {todaysDate}"

sendMailCommand = f"""
cat <<'EOF' - /iics_pmroot/Temp/test_zp_count_mail_body.html | /usr/sbin/sendmail -t
To: {zp_receiver_email}
Cc: {zp_cc_email}
Subject: {subject}
Content-Type: text/html

EOF
"""

#send mail using unix
unix(sendMailCommand)