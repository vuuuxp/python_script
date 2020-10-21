#!/usr/bin/env python
import yaml
import logging
import cassandra.auth
import cassandra.cluster
import pandas as pd
from collections import defaultdict
from cassandra.policies import DCAwareRoundRobinPolicy

logging.getLogger("cassandra").setLevel(logging.WARNING)
logging.basicConfig(filename='persistentGeneratorAudit.log', format='%(message)s', level=logging.INFO, filemode='a')
logger = logging.getLogger(__name__)

class CassandraConnection:
    def __init__(self, config):
        config = config['cassandra']
        auth_provider = cassandra.auth.PlainTextAuthProvider(
            username=config['username'],
            password=config['password'])

        self.cluster = cassandra.cluster.Cluster(
            protocol_version=3,
            contact_points=config['hosts'],
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=config['local_dc']),
            auth_provider=auth_provider)

        self.session = self.cluster.connect(config['keyspace'])

    def getSession(self):
        return self.session


class PAGenerator:
    def __init__(self):
        with open('config.yaml', 'r') as f:
            config = yaml.load(f)
        self.db_cassandra = CassandraConnection(config).getSession()

    def generate_csv_sql(self):

        with open('config.yaml', 'r') as f:
            config = yaml.load(f)

        df = pd.DataFrame.from_csv('input.csv', index_col=None)
        region_dct = dict(tuple(df.groupby('Region'))) # convert dataframe to tuple and then to dict.
        region_list = ['EMEA', 'APAC', 'AMER']

        for region in region_list:
            size = region_dct[region].shape
            weightMap = defaultdict(tuple)
            for i in range(size[0]):
                inptrow = []
                for j in range(size[1]):
                    inptrow.append(region_dct[region].iloc[i, j])
                weightMap[inptrow[0]] = (inptrow[2], inptrow[1])

            if region == 'AMER':
                binsSize = [0, 0, 0, 0, 0, 0, 0, 0] # TO-DO - get bin sizes from cassandra.
                siteDist = [[], [], [], [], [], [], [], []]
                primary = config['region']['AMER']['primary_dc']
                seconday = config['region']['AMER']['secondary_dc']
                serverList = dict(config['region']['AMER']['serverList'])
                bck_serverList = dict(config['region']['AMER']['bck_serverList'])
                sec_serverList = dict(config['region']['AMER']['sec_serverList'])
                sec_bck_serverList = dict(config['region']['AMER']['sec_bck_serverList'])

            if region == 'EMEA':
                binsSize = [0, 0, 0] # TO-DO - get bin sizes from cassandra.
                siteDist = [[], [], []]
                primary = config['region']['EMEA']['primary_dc']
                seconday = config['region']['EMEA']['secondary_dc']
                serverList = dict(config['region']['EMEA']['serverList'])
                bck_serverList = dict(config['region']['EMEA']['bck_serverList'])
                sec_serverList = dict(config['region']['EMEA']['sec_serverList'])
                sec_bck_serverList = dict(config['region']['EMEA']['sec_bck_serverList'])

            if region == 'APAC':
                binsSize = [0, 0] # TO-DO - get bin sizes from cassandra.
                siteDist = [[], []]
                primary = config['region']['APAC']['primary_dc']
                seconday = config['region']['APAC']['secondary_dc']
                serverList = dict(config['region']['APAC']['serverList'])
                bck_serverList = dict(config['region']['APAC']['bck_serverList'])
                sec_serverList = dict(config['region']['APAC']['sec_serverList'])
                sec_bck_serverList = dict(config['region']['APAC']['sec_bck_serverList'])

            for k, v in weightMap.items(): # simple min bucket distribution.
                index = binsSize.index(min(binsSize))
                binsSize[index] += int(v[0])
                siteDist[index].append((k,v[1]))

            sqlfile = open(region+'.sql', 'w')
            for siteIdList in siteDist:
                for siteID in siteIdList:
                    sql = "insert into ks_global_pda.appnametohostnamemappingtable (id,app_name,app_type_name,cn_hostname,last_reload_time,org_id) values ('" +\
                          str(siteID[0]) + "_" + primary + "','" + str(siteID[1]) + "__" + str(siteID[0]) + "','" + str(siteID[1]) + "','" + str(serverList[siteDist.index(siteIdList)]) + "','" + "null" + "','" + str(siteID[0]) + "'" + ");" + "\n"

                    backup_sql = "insert into ks_global_pda.appnametohostnamemappingtable (id,app_name,app_type_name,cn_hostname,last_reload_time,org_id) values ('" + \
                              str(siteID[0]) + "_" + primary + "_backup','" + str(siteID[1]) + "__" + str(siteID[0]) + "','" + str(siteID[1]) + "','" + str(bck_serverList[siteDist.index(siteIdList)]) + "','" + "null" + "','" + str(siteID[0]) + "'" + ");" + "\n"

                    sec_sql = "insert into ks_global_pda.appnametohostnamemappingtable (id,app_name,app_type_name,cn_hostname,last_reload_time,org_id) values ('" +\
                          str(siteID[0]) + "_" + seconday + "','" + str(siteID[1]) + "__" + str(siteID[0]) + "','" + str(siteID[1]) + "','" + str(sec_serverList[siteDist.index(siteIdList)]) + "','" + "null" + "','" + str(siteID[0]) + "'" + ");" + "\n"

                    sec_back_backup_sql = "insert into ks_global_pda.appnametohostnamemappingtable (id,app_name,app_type_name,cn_hostname,last_reload_time,org_id) values ('" + \
                              str(siteID[0]) + "_" + seconday + "_backup','" + str(siteID[1]) + "__" + str(siteID[0]) + "','" + str(siteID[1]) + "','" + str(sec_bck_serverList[siteDist.index(siteIdList)]) + "','" + "null" + "','" + str(siteID[0]) + "'" + ");" + "\n"

                    sqlfile.write(sql)
                    sqlfile.write(backup_sql)
                    sqlfile.write(sec_sql)
                    sqlfile.write(sec_back_backup_sql)
            sqlfile.close()

            csvrow = "id,app_name,app_type_name,cn_hostname,last_reload_time,org_id"
            # print (csv)d
            with open(region+'.csv', 'w') as csvfile:
                for siteIdList in siteDist:
                    for siteID in siteIdList:
                        csvrow = str(siteID[0]) + "_" + primary + "," + str(siteID[1]) + "__" + str(siteID[0]) + "," + str(siteID[1]) + "," + str(serverList[siteDist.index(siteIdList)]) + "," + "null" + "," + str(siteID[0]) + "\n"
                        backup_csvrow = str(siteID[0]) + "_" + primary + "_backup," + str(siteID[1]) + "__" + str(siteID[0]) + "," + str(siteID[1]) + "," + str(bck_serverList[siteDist.index(siteIdList)]) + "," + "null" + "," + str(siteID[0]) + "\n"
                        sec_csvrow = str(siteID[0]) + "_" + seconday + "," + str(siteID[1]) + "__" + str(siteID[0]) + "," + str(siteID[1]) + "," + str(sec_serverList[siteDist.index(siteIdList)]) + "," + "null" + "," + str(siteID[0]) + "\n"
                        sec_backup_csvrow = str(siteID[0]) + "_" + seconday + "_backup," + str(siteID[1]) + "__" + str(siteID[0]) + "," + str(siteID[1]) + "," + str(sec_bck_serverList[siteDist.index(siteIdList)]) + "," + "null" + "," + str(siteID[0]) + "\n"
                        csvfile.write(csvrow)
                        csvfile.write(backup_csvrow)
                        csvfile.write(sec_csvrow)
                        csvfile.write(sec_backup_csvrow)
            csvfile.close()


if __name__ == "__main__":
    PAG = PAGenerator().generate_csv_sql()


#chmod +x myfile.py  chmod +x PerGenerator.py
#./PerGenerator.py