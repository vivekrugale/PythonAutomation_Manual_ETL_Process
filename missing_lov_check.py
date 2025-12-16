import cx_Oracle
import pandas as pd
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

#Function to execute unix commands.
def unix(command):
    os.system(command)

#Function to connect and query Oracel DB
def queryDB(username, password, hostname, port, sid, query):
    try:
        dsn = cx_Oracle.makedsn(hostname, port, sid=sid)
        connection = cx_Oracle.connect(username, password, dsn)

    except cx_Oracle.DatabaseError as e:
        print("Error connecting to Oracle DB: ", e)

    try:
        cursor = connection.cursor()
        cursor.execute(query)

        columns = [col[0] for col in cursor.description]

        data = cursor.fetchall()

        if not data:
            return "No missing records"
        else:
            df_queryOutput = pd.DataFrame(data, columns=columns)
            print(df_queryOutput)

            df_queryOutput = df_queryOutput.to_html(index=False, classes='outputTable')
            df_queryOutput = df_queryOutput.replace('class="dataframe outputTable"','class="outputTable"') 
            
            return df_queryOutput

    except cx_Oracle.DatabaseError as e:
        print(f"Query execution failed: {e}")

    finally:
        cursor.close()
        connection.close()

#APAC
query = """
    select distinct da_area_code, substr(COD_ID_ONEKEY,1,INSTR(COD_ID_ONEKEY,'.',1)-1) slov, COD_ID_ONEKEY
    from cml_dl_stg.ceg_cml_stg_lov_code
    where COD_ID_ONEKEY is not null and exists (select 1 from cml_dl_land.lov_files_load where ceg_code is not null and country_code = da_area_code and 
    substr(COD_ID_ONEKEY,1,INSTR(COD_ID_ONEKEY,'.',1)-1)=substr(ceg_code,1,INSTR(ceg_code,'.',1)-1)) and cod_id_onekey not in ('SP.WBE.85','FAC.WUK.00') and
    not exists(select 1 from cml_dl_land.lov_files_load where ceg_code is not null and country_code = da_area_code and cod_id_onekey = ceg_code)
    order by 1,2
    """
vars = ["username","password","hostname","port","sid"]
path = "/iics_pmroot/Profiles/gblsfa_apac_automation.profile"
apacCredentials = readProfile(vars, path)
username = apacCredentials.get("username", None)
password = apacCredentials.get("password", None)
hostname = apacCredentials.get("hostname", None)
port = apacCredentials.get("port", None)
sid = apacCredentials.get("sid", None)

#Connect to APAC DB
apac_output = queryDB(username, password, hostname, port, sid, query)

#EURAM
vars = ["username","password","hostname","port","sid","receiver_email"]
path = "/iics_pmroot/Profiles/gblsfa_euram_automation.profile"
euCredentials = readProfile(vars, path)
username = euCredentials.get("username", None)
password = euCredentials.get("password", None)
hostname = euCredentials.get("hostname", None)
port = euCredentials.get("port", None)
sid = euCredentials.get("sid", None)
receiver_email = euCredentials.get("receiver_email", None)

#Connecting to EU DB
eu_output = queryDB(username, password, hostname, port, sid, query)

query_za_hcp = """
select distinct l.SERVICENAME as SERVICE,'Other' as Account_type,'Other' as SPECIALTY,o.*
from CML_DL_STG.medp_cml_stg_service_lov l left join CML_DL_STG.medp_cml_stg_srv_contyp_spcl_prsn o on l.SERVICENAME=o.SERVICE
where l.RECORDTYPECODE='2' and o.service is null
"""
#Query EU DB for ZA HCP
za_hcp_output = queryDB(username, password, hostname, port, sid, query_za_hcp)

query_za_hco = """
select distinct l.SERVICENAME as SERVICE,'Other' as Account_type,'Other' as SPECIALTY--,o.*
from CML_DL_STG.medp_cml_stg_service_lov l left join CML_DL_STG.medp_cml_stg_srv_acctyp_spcl_org o on l.SERVICENAME=o.SERVICE
where l.RECORDTYPECODE='1' and o.service is null
"""
#Query EU DB for ZA HCO
za_hco_output = queryDB(username, password, hostname, port, sid, query_za_hco)

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
<p class="content">Below are the missing LOVs found for respective regions.</p>
<p class="content">APAC: <br>{apac_output}</p>
<p class="content">EURAM: <br>{eu_output}</p>
<p class="content">ZA HCP Service: <br>{za_hcp_output}</p>
<p class="content">ZA HCO Service: <br>{za_hco_output}</p>
<p class="content">LATAM: Not Applicable</p>
<p style="margin-top: 30px;">Thanks and regards.</p>
<p>Note: This is an Auto-Generated Email.</p>
</body>
</html>
"""

today = date.today()
todaysDate = today.strftime("%B %d, %Y")

#Save the mail body as .html file in Temp folder to fire email with linux command.
mail_body_file = "/iics_pmroot/Temp/missingLOV_report_mail_body.html"
with open(mail_body_file, "w", encoding='utf-8') as file:
    file.writelines('<meta charset="UTF-8">\n')
    file.write(mail_body)

subject = f"Weekly missing LOV check - {todaysDate}"

sendMailCommand = f"""
cat <<'EOF' - /iics_pmroot/Temp/missingLOV_report_mail_body.html | /usr/sbin/sendmail -t
To: {receiver_email}
Subject: {subject}
Content-Type: text/html

EOF
"""

#send mail using unix
unix(sendMailCommand)