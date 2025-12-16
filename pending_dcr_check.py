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
            return "No Pending DCRs"
        else:
            df_queryOutput = pd.DataFrame(data, columns=columns)
            df_queryOutput.rename(columns={"GAL_AREA_CODE": "COUNTRY"}, inplace=True)

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
with r as
(
SELECT REQUEST_ID_CLIENT,REQUEST_DATE,DA_INSERTED_DATETIME,DA_UPDATED_DATETIME FROM CML_DL_STG.CEG_CML_STG_VAL_REQ_ACTV
UNION ALL
SELECT REQUEST_ID_CLIENT,REQUEST_DATE,DA_INSERTED_DATETIME,DA_UPDATED_DATETIME FROM CML_DL_STG.CEG_CML_STG_VAL_REQ_WKPLC
)
select
p.gal_area_code, COUNT(*) AS COUNT
from VVA_MIRROR_PUB.pub_data_change_request p 
inner join r on p.id=r.REQUEST_ID_CLIENT
left join CML_DL_STG.CEG_CML_STG_VAL_RESPONSE re on r.REQUEST_ID_CLIENT=re.REQUEST_ID
where re.response_date is null and r.da_updated_datetime<= sysdate-15 and r.DA_INSERTED_DATETIME>'23-Nov-2023' and r.request_date>'2023-11-23T00:00:01'
group by p.gal_area_code order by 2 desc
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
<p class="content">Below are the country-wise pending DCRs in EURAM:</p>
<p class="content">{output}</p>
<p style="margin-top: 30px;">Thanks and regards.</p>
<p>Note: This is an Auto-Generated Email.</p>
</body>
</html>
"""

today = date.today()
todaysDate = today.strftime("%B %d, %Y")

#Save the mail body as .html file to fire email with linux command.
mail_body_file = "/iics_pmroot/Temp/pending_DCR_Check_mailBody.html"
with open(mail_body_file, "w") as file:
    file.write(mail_body)

subject = f"EU - Daily Pending DCR Check - {todaysDate}"

sendMailCommand = f"""
cat <<'EOF' - /iics_pmroot/Temp/pending_DCR_Check_mailBody.html | /usr/sbin/sendmail -t
To: {receiver_email}
Subject: {subject}
Content-Type: text/html

EOF
"""

#send mail using unix
unix(sendMailCommand)