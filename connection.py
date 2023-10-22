import pymysql
import os

ENDPOINT="lolesport.cv2hmfnelsdh.us-east-2.rds.amazonaws.com"
PORT=3306
USER="admin"
REGION="us-east-2c"
DBNAME= ""
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

#gets the credentials from .aws/credentials

try:
    conn =  pymysql.connect(host=ENDPOINT, user=USER, passwd='12345678', port=PORT, database=DBNAME, ssl_ca='us-east-2-bundle.pem')
    cur = conn.cursor()
    cur.execute("""Select Now()""")
    query_results = cur.fetchall()
    print(query_results)
except Exception as e:
    print("Database connection failed due to {}".format(e))
