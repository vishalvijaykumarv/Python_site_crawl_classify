import os, sys, logging, pymysql, configparser
import fun.classified_dynamo_remove as cdr
from scripts.configreader import RDS_config
# Connection for RDS


logger = logging.getLogger()
logger.setLevel(logging.INFO)

dates = str(sys.argv[1])
domain = str(sys.argv[2])
cdr.remove_from_dynamo(dates)

try:
    conn = pymysql.connect(host=RDS_config['host_add'], user=RDS_config['username'],
                           passwd=RDS_config['password'], db=RDS_config['database'], connect_timeout=5)
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()


def scrape_handler():
    del_contains = "delete from contains where site_id in (select distinct site_id from site where page_url like '%" + domain + "%');"
    del_site = "delete from site where page_url like '%" + domain + "%';"
    del_site_category = "delete from site_category where domain = '" + domain + "';"

    print(del_contains)
    del_contains = conn.cursor().execute(del_contains)
    conn.commit()
    print("Removed Old " + str(domain) + " Relations from contains: " + str(del_contains))

    print(del_site)
    del_site = conn.cursor().execute(del_site)
    conn.commit()
    print("Removed Old " + str(domain) + " URLS from Site: " + str(del_site))

    print(del_site_category)
    del_site_category = conn.cursor().execute(del_site_category)
    conn.commit()
    print("Removed Old " + str(domain) + " Relations from Site_category: " + str(del_site_category))


scrape_handler()
