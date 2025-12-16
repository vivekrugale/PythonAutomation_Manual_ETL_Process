import cx_Oracle
import pandas as pd
from datetime import date
import os

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

def queryDB(username, password, hostname, port, sid, query):
    try:
        dsn = cx_Oracle.makedsn(hostname, port, sid=sid)
        connection = cx_Oracle.connect(username, password, dsn)
        # print('Connection successful')

    except cx_Oracle.DatabaseError as e:
        print("ERROR: ", e)

    try:
        cursor = connection.cursor()
        cursor.execute(query)

        columns = [col[0] for col in cursor.description]

        data = cursor.fetchall()

        if not data:
            return "No missing HCOs"
        else:
            df_queryOutput = pd.DataFrame(data, columns=columns)

            df_queryOutput.loc[len(df_queryOutput.index)] = ['Total count', df_queryOutput['COUNT'].sum()]

            df_queryOutput = df_queryOutput.to_html(index=False, classes='outputTable')
            df_queryOutput = df_queryOutput.replace('class="dataframe outputTable"','class="outputTable"') 
            
            return df_queryOutput

    except cx_Oracle.DatabaseError as e:
        print(f"Query execution failed: {e}")

    finally:
        cursor.close()
        connection.close()
        # print("Database connection closed.")

query = """
SELECT a.country_code, count(a.account_onekey_id) as COUNT FROM VVA_APP_STG.VVA_STG_ACCOUNT a 
left join vva_MIRROR_PUB.PUB_ACCOUNT pa on a.account_onekey_id=pa.external_id_vod 
where a.account_status='Active' and pa.external_id_vod is null 
and a.country_code NOT IN ('DZ','MA','UA','ZA','CH','AT','HU','AE','BH','KW','OM','SA','QA') 
group by a.country_code order by 2 desc
"""
vars = ["username","password","hostname","port","sid","receiver_email"]
path = "/iics_pmroot/Profiles/gblsfa_euram_automation.profile"
euCredentials = readProfile(vars, path)
username = euCredentials.get("username", None)
password = euCredentials.get("password", None)
hostname = euCredentials.get("hostname", None)
port = euCredentials.get("port", None)
sid = euCredentials.get("sid", None)
receiver_email = euCredentials.get("receiver_email", None)

output = queryDB(username, password, hostname, port, sid, query)

mail_body = f"""
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, intial-scale=1.0">
<style>
    body {{
        font-family: "Aptos", Arial, Helvetica, sans-serif;
        font-size: 14.5px;
        line-height: 1.5;
        margin: 0;
        padding: 0;
    }}
    p{{
        margin: 0;
    }}
    .content {{
        margin: 10px 0;
    }}
    table{{
        width: auto;
        border-collapse: collapse;
        table-layout: auto;
        padding-left: 8px;
        padding-right: 8px;
        text-align: left;
    }}
    th, td{{
        font-size: 14px;
    }}
</style>
</head>
<body>
<p>Hi Team,</p>
<p class="content">Below are the EU HCOs found which are present in stage table but are missing in Veeva:</p>
<p class="content">{output}</p>
<p style="margin-top: 30px;">Thanks and regards.</p>
<p>Note: This is an Auto-Generated Email.</p>
</body>
</html>
"""

today = date.today()
todaysDate = today.strftime("%B %d, %Y")

#Save the mail body as .html file to fire email with linux command.
mail_body_file = "/iics_pmroot/Temp/missing_HCO_Check_mailBody.html"
with open(mail_body_file, "w") as file:
    file.write(mail_body)

subject = f"EU - Daily missing HCO check - {todaysDate}"

sendMailCommand = f"""
cat <<'EOF' - /iics_pmroot/Temp/missing_HCO_Check_mailBody.html | /usr/sbin/sendmail -t
To: {receiver_email}
Subject: {subject}
Content-Type: text/html

EOF
"""

#send mail using unix
unix(sendMailCommand)