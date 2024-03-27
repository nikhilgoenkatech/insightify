import io, csv
import math
import time
import json
import logging
import requests
import tempfile
import datetime
from pprint import pprint
from pathlib import Path
from random import randrange
from dynatrace_extension import Extension, Status, StatusValue


class ExtensionImpl(Extension):

    class problem_mgmt_zone:
        def __init__(self):
          self.mgmt_zone = 0
          self.problems = 0
          self.rootCause = 0
          self.application=0
          self.service=0
          self.infrastructure=0
          self.error=0
          self.avalability=0
          self.performance=0
          self.resource=0
          self.custom=0
          self.mttr_rca=0
          self.mttr_wo_rca=0
          self.mttr_rca_list=[]
          self.mttr_wo_rca_list=[]

    #When to collect the first sample of problem data
    COLLECT_PROBLEM_DATA=1

    DASHBOARDS="dashboards"
    GET_EXISTING_DASHBOARD="dashboards?owner=dynatraceone"
    INGEST_METRICS = "metrics/ingest"
    GET_DATA_INSERTED_DATA_POINT="metrics/query?metricSelector=record_insertion_time:filter(and(or(eq(config_id,entityID))))&resolution=1d&from=now-1y"

    #PROBLEMS API
    PROBLEMS="problems?pageSize=500&from=starttime&to=endtime"
    SPECIFIC_PROBLEMS="problems?pageSize=500&from=starttime&to=endtime&problemSelector=managementZones%28%22mgmt_zone_name%22%29"
    TEST_PROBLEMS="problems?pageSize=1&from=-2h"

    def initialize(self):
        self.extension_name = "insightify"
        try:
                #Flag to indicate if earlier record push value has been pulled
                self.problem_time_retrieve_flag = 0
                #Other variables
                self.pull_prb_data_iterations = 0
                self.get_last_inserted_data_set = 0
                self.dashboard_created = 0
                self.problem_dashboard_created = 0

                self.problems_mgmt_zone = {}

        except Exception as e:
                self.logger.exception("Exception while running initialize", str(e), exc_info=True)

        finally:
                self.logger.info("Successful execution initialize ")

    # *********************************************************************    
    #           Function to get metrics/SLO using API V2 endpoint
    # *********************************************************************    
    def dtApiV2GetQuery(self, apiendpoint, endpoint_detail):
      self.logger.info("In dtApiV2GetQuery")
      try:
        data = {}
        config_url=endpoint_detail["url"]
        password=endpoint_detail["token"]
        self.logger.info("config_url -> " + config_url)
        self.logger.info("Password= " + password)
        
        config_url = config_url.replace("v1","v2")
        query = str(config_url) + apiendpoint
        self.logger.info("Query -> " + query)
        
        get_param = {'Accept':'application/json', 'Authorization':'Api-Token {}'.format(password)}
        populate_data = requests.get(query, headers = get_param, verify=False)
        self.logger.info(populate_data)

        if populate_data.status_code >=200 and populate_data.status_code <= 400:
          data = populate_data.json()

        elif populate_data.status_code == 401:
          msg = "Auth Error"

      except Exception as e:
        self.logger.exception("in dtApiV2GetQuery ", str(e))

      finally:
        self.logger.info("In dtApiV2GetQuery")  
        return data

    # *********************************************************************
    #           Function to get metrics using API V2 endpoint
    # *********************************************************************
    def dtApiV2GetMetricDataPoint(self, endpoint, endpoint_config):
      #try:
        self.logger.info("In dtApiV2GetMetricDataPoint")
        self.logger.info(endpoint_config)
        data = {}

        config_url = endpoint_config["confurl"]
        conf_password = endpoint_config["conftoken"]
         
        query = str(config_url) + endpoint
        get_param = {'Accept':'application/json', 'Authorization':'Api-Token {}'.format(conf_password)}
        populate_data = requests.get(query, headers = get_param, verify=False)
        self.logger.info(query)
        self.logger.info(populate_data)

        if populate_data.status_code >=200 and populate_data.status_code <= 400:
          data = populate_data.json()

        elif populate_data.status_code == 401:
          msg = "Auth Error"

      #except Exception as e:
      #  self.logger.info("in dtApiV2GetMetricDataPoint", str(e))

      #finally:
      #  self.logger.info("Succesfully completion dtApiV2GetMetricDataPoint")
        return data

    # *******************************************************************************    
    #            Function to push metrics using Ingest Metrics endpoint
    #      This is pushed to ingest host,dem and ddu units per management zone 
    # *******************************************************************************    
    def dtApiIngestMetrics(self, endpoint, payload, endpoint_config):
      try:
        self.logger.info("In dtApiIngestMetrics ")
        data = {}

        config_url = endpoint_config["confurl"] 
        conf_password = endpoint_config["conftoken"] 
        config_url = config_url.replace("v1","v2")

        query = str(config_url) + endpoint
        post_param = {'Content-Type':'text/plain;charset=utf-8', 'Authorization':'Api-Token {}'.format(conf_password)}
        
        populate_data = requests.post(query, headers = post_param, data = payload, verify=False)

        self.logger.info(query)
        self.logger.info(payload)
        self.logger.info(populate_data)
        self.logger.info(populate_data.content)

        if populate_data.status_code >=200 and populate_data.status_code <= 400:
          data = populate_data.json()

        elif populate_data.status_code == 401:
          msg = "Auth Error"

      except Exception as e:
        self.logger.exception("Exception in dtApiIngestMetrics ", str(e))

      finally:
        self.logger.info("Succesfully completed dtApiIngestMetrics")  
        return data


    # *******************************************************************************    
    #           Function to post data using API v1 endpoint
    # *******************************************************************************    
    def dtApiV1PostQuery(self, endpoint, payload, endpoint_config):
      try:
        self.logger.info("In dtApiV1PostQuery")  
        data = {}

        config_url = endpoint_config["confurl"]
        conf_password = endpoint_config["conftoken"] 
        config_url = config_url.replace("v2","config/v1")
        
        query = str(config_url) + endpoint
        post_param = {'Content-Type':'application/json', 'Authorization':'Api-Token {}'.format(conf_password)}
        populate_data = requests.post(query, headers=post_param, data=json.dumps(payload), verify=False)
        self.logger.info(populate_data)

        if populate_data.status_code >=200 and populate_data.status_code <= 400:
          data = populate_data.json()

        elif populate_data.status_code == 401:
          msg = "Auth Error"

      except Exception as e:
        self.logger.info("In dtApiV1PostQuery: " + str(e))  

      finally:
        self.logger.info("Succesfully completed dtApiV1PostQuery")

    # *******************************************************************************    
    #           Function query it executes every minute 
    # *******************************************************************************    
    def query(self):
        """
        The query method is automatically scheduled to run every minute
        """
        self.logger.info("query method started for insightify.")
        for endpoint in self.activation_config["endpoints"]:
            #try:
                url = endpoint["url"]
                password = endpoint["token"]
                get_problem_data = endpoint["get_problem_data"]
                get_ff_data = endpoint["get_ff_data"]
                get_problem_data_mgmt_zone = endpoint["get_problem_data_mgmt_zone"]
                confurl = endpoint["confurl"]
                conf_password = endpoint["conftoken"]
                prb_management_zone_list = endpoint["management_zone_name"]
                prb_report_date = endpoint["get_generate_report"]
                activegate_endpoint = endpoint["ag_endpoint"]
                converted_to_incident_duration = endpoint["problem_to_incident_duration"]
                #Convert mins to ms (as problems api v2 reports resolution time in ms)
                converted_to_incident_duration = int(converted_to_incident_duration)*60000


                #Local copy of the variable to identify when to pull problem data
                problem_time_interval = self.COLLECT_PROBLEM_DATA 

                self.logger.debug(f"Running endpoint with url '{url}'")
                #self.logger.info("config id = " + self.monitoring_config_id)

                # Your extension code goes here, e.g.
                
                entityID = self.monitoring_config_id
                self.logger.info("\n\n\n" + entityID)
                # if self.dashboard_created == 0:
                #     dashboard_file = Path(__file__).parent / "dashboard.json"
                #     fp = open(dashboard_file,'r')
                #     dashboard_json = json.loads(fp.read())

                #     payload = json.dumps(dashboard_json).replace('$1',endpoint) 
                #     dashboard_json = json.loads(payload)
                #     dashboard_name = dashboard_json["dashboardMetadata"]["name"]
                #     dashboards = self.dtConfApiv1(GET_EXISTING_DASHBOARD)

                #     self.logger.info("Found dashboards: " + str(len(dashboards)))
                #     #Check if the dashboard is already present
                #     if len(dashboards) > 0:
                #     dashboardList = dashboards["dashboards"]
                    
                #     for dashboard in dashboardList:
                #         if dashboard["name"].__eq__(dashboard_name):
                #             self.logger.info("Found a dashboard already by that configuration. Skipping.. " + dashboard["name"])
                #             self.dashboard_created = 1
                #             continue

                #     #Verifying that the dashboard is not already created
                #     if (self.dashboard_created == 0):
                #     self.logger.info("Will create a dashboard " + dashboard_name)
                #     self.dtApiV1PostQuery(DASHBOARDS, dashboard_json)
                #     self.dashboard_created = 1
                    
                #if (self.get_problem_data_mgmt_zone == "Yes" and self.problem_dashboard_created == 0):
                #    dashboard_file = Path(__file__).parent / "operation_dashboard.json"
                #    fp = open(dashboard_file,'r')
                #    dashboard_json = json.loads(fp.read())
                #    
                #    payload = json.dumps(dashboard_json).replace('$1',endpoint) 
                #    dashboard_json = json.loads(payload)
                #    dashboard_name = dashboard_json["dashboardMetadata"]["name"]
                #    dashboards = self.dtConfApiv1(self.GET_EXISTING_DASHBOARD)
                #
                #    self.logger.info("(problem_dashboard_check) Dashboard founds: " + str(len(dashboards))) 
                    #Check if the dashboard is already present
                    #if len(dashboards) > 0:
                    #dashboardList = dashboards["dashboards"]
                    
                    #    for dashboard in dashboardList:
                    #        self.logger.info(dashboard["name"]) 
                    #        if dashboard["name"].__eq__(dashboard_name):
                    #            self.logger.info("Found a dashboard already by that configuration. Skipping.. " + dashboard["name"])
                    #            self.problem_dashboard_created = 1
                    #            continue 
                
                    #Verifying that the dashboard is not already created
                    #if (self.problem_dashboard_created == 0):
                    #    self.logger.info("Will create a dashboard " + dashboard_name)
                    #    self.dtApiV1PostQuery(self.DASHBOARDS, dashboard_json)

                    #Create benefit value realisation dashboard 
                    #dashboard_file = Path(__file__).parent / "benefit_realisation_report.json"
                    #fp = open(dashboard_file,'r')
                    #dashboard_json = json.loads(fp.read())

                    #dashboard_name = dashboard_json["dashboardMetadata"]["name"]
                    #self.logger.info("Will also create " + dashboard_name)

                    #payload = json.dumps(dashboard_json).replace('$1',endpoint)
                    #dashboard_json = json.loads(payload)
                    #self.dtApiV1PostQuery(self.DASHBOARDS, dashboard_json)

                    #Create problem trend analysis
                    #dashboard_file = Path(__file__).parent / "problem_trend_and_analysis.json"
                    #fp = open(dashboard_file,'r')
                    #dashboard_json = json.loads(fp.read())

                    #dashboard_name = dashboard_json["dashboardMetadata"]["name"]
                    #payload = json.dumps(dashboard_json).replace('$1',endpoint)
                    #dashboard_json = json.loads(payload)
                    #self.dtApiV1PostQuery(self.DASHBOARDS, dashboard_json)

                    #self.problem_dashboard_created = 1

                #Verify if there are any records inserted for the problem data before
                try:
                    self.logger.info("problem_time_retrieve_flag " + str(self.problem_time_retrieve_flag))
                    
                    if self.get_last_inserted_data_set:
                        self.logger.info("Got some value for last_inserted_data_set")
                    else:
                        self.logger.info("No value for last_inserted_data_set below")
                        self.get_last_inserted_data_set = 0

                    if self.problem_time_retrieve_flag == 0:
                        # Get the last inserted record timestamp
                        query = self.GET_DATA_INSERTED_DATA_POINT.replace("entityID", entityID)
                        self.get_last_inserted_data_set = self.dtApiV2GetMetricDataPoint(query, endpoint)
                
                    if self.get_last_inserted_data_set:
                        self.logger.info("Will identify the last inserted record timestamp")
                        for result_obj in self.get_last_inserted_data_set.get('result', []):
                            for data_obj in result_obj.get('data', []):
                                if data_obj['dimensions'][0] == entityID:
                                    self.logger.info("Found the custom-device " + data_obj['dimensions'][0])
                                    for j, value in enumerate(data_obj.get('values', [])):
                                        if value:
                                            self.logger.info(value)
                                            last_inserted_record_time = value
                                            self.logger.info("Last inserted record time: " + str(last_inserted_record_time))
                
                                            # Setup the next expected insertion time (using the last record timestamp)
                                            now = time.time()
                                            days = {'Last 30 days': 30, 'Last 60 days': 60, 'Last 90 days': 90}.get(prb_report_date, 365)
                                            expected_next_data_insertion = last_inserted_record_time + (days * 86400)
                                            problem_time_interval = int((expected_next_data_insertion - now) / 60)
                                            self.problem_time_retrieve_flag = 1
                                            self.logger.info("expected_next_data " + str(expected_next_data_insertion))
                                            self.logger.info("problem_time_interval = " + str(problem_time_interval))
                    else:
                        self.logger.info("No records found and looks like first iteration")
                
                    self.logger.info("problem_time_interval " + str(problem_time_interval))
                    self.logger.info("self.pull_prb_data_iterations " + str(self.pull_prb_data_iterations))
                
                    if get_problem_data == "Yes" and self.pull_prb_data_iterations >= problem_time_interval:
                        # Collect problem data
                        self.logger.info("Pulling and inserting data ")
                        self.pull_prb_data_iterations = 0
                        self.pull_prb_data(self.logger, entityID, self.problems_mgmt_zone, prb_management_zone_list, prb_report_date, activegate_endpoint, endpoint)
                    else:
                        self.pull_prb_data_iterations += 1
                
                except Exception as e:
                    self.logger.exception("Exception encountered in query " + str(e))
                finally:
                    self.logger.info("Execution completed query function")
                
                self.logger.info("query method ended for insightify.")
  
    # *******************************************************************************    
    #           Function to pull the generated problems data 
    # *******************************************************************************

    def pull_prb_data(self, logger, entityID, problems_mgmt_zone, prb_management_zone_list, prb_report_date, ag_endpoint, endpoint_detail):
      #try:
          self.logger.info("In pull_prb_data")
        
          now = time.time()
          days = {'Last 30 days': 30, 'Last 60 days': 60, 'Last 90 days': 90}.get(prb_report_date, 365)
          end_date = now - (days * 86400)
    
          start_time_ms = int(end_date) * 1000
          end_time_ms = int(now) * 1000
   
          if prb_management_zone_list != "" and prb_management_zone_list != "all" and prb_management_zone_list != "All":
            PRB_QUERY = self.SPECIFIC_PROBLEMS.replace("starttime", str(start_time_ms))
            PRB_QUERY = PRB_QUERY.replace("endtime", str(end_time_ms))
            PRB_QUERY = PRB_QUERY.replace("mgmt_zone_name",prb_management_zone_list)

          else:    
            PRB_QUERY = self.PROBLEMS.replace("starttime", str(start_time_ms))
            PRB_QUERY = PRB_QUERY.replace("endtime", str(end_time_ms))

          data = self.dtApiV2GetQuery(PRB_QUERY, endpoint_detail)
          data_to_be_added = 0

          if data is not None:
            if len(data) >= 0:
              try:
                nextPageKey = data['nextPageKey']

                while nextPageKey != "":
                  query = "problems?nextPageKey=" + nextPageKey
                  self.logger.info("Retrieve data " + query)

                  result = self.dtApiV2GetQuery(query, endpoint_detail)
                  nextPageKey = result['nextPageKey']
                  data["problems"] += result["problems"]
                  data_to_be_added = 1

              except KeyError:
                  if data_to_be_added == 1:
                    data["problems"] += result["problems"]
                    data_to_be_added = 0 

              except Exception as e:
                  self.logger.exception("Exception encountered in pull_prb_data" + str(e))

              self.logger.info("Getting some data")
              mean_resolution_time,problems_mgmt_zone = self.populate_problem_data(entityID, data["problems"], problems_mgmt_zone, ag_endpoint, endpoint_detail)

#        except Exception as e:
#          self.logger.exception("Exception encountered in pull_prb_data " + str(e))

#        finally:
#         self.logger.info("Execution completed pull_prb_data")


    # ********************************************************************************************************
    #        Function to initialize the csv file which will be dumped as logs 
    # ********************************************************************************************************
    def initialize_csv_header(self):
        #try:
          self.logger.info("In initialize_csv_header")  

          csv_data = ""
          csv_data="config_id,Endpoint Name,status,management.zone,Problem ID,Problem Link,problem.title,impact.level,severity.level,RCA or no RCA, MTTR(in hours)\n"

        #except Exception as e:
#          self.logger.exception("Exception encountered in initialize_csv_header: ", str(e))  

#        finally:
          self.logger.info("Succesfully executed initialize_csv_header")  
          return csv_data 

    # ********************************************************************************************************
    #        Function to slice and dice problem trend and push metrics 
    # ********************************************************************************************************
    def slice_and_dice_problem_trend(self, logger, csv_data, endpoint_config):
       try:
           self.logger.info("In slice_and_dice_problem_trend")
           data = []
           f = io.StringIO(csv_data)
           reader = csv.DictReader(f)

           for row in reader:
             data.append(row)
           # Extract the timestamp column as a list of datetime objects
           timestamps = [datetime.datetime.fromtimestamp(int(row['starttime'])) for row in data]
           
           # Group the data by month,year, problem title and management zone, keeping track of the number of occurrences and the sum of downtime 
           result = {}
           for row, timestamp in zip(data, timestamps):
               key = (timestamp.year, timestamp.month, row["problem.title"], row["management.zone"],row["config_id"])
               if key not in result:
                   result[key] = {"count": 0, "downtime": []}
               result[key]["count"] += 1
               result[key]["downtime"].append(float(row["mttr"]))
           
           # Format the result as a list of dictionaries, with keys being fieldnames
           result_data = []
           for key, value in result.items():
               year, month, column_value,zone,entityId = key
               count = value["count"]
               total_downtime = sum(value["downtime"])
               result_data.append({
                   "entityId":entityId,
                   "year": year,
                   "month": month,
                   "zone":zone,
                   "column_value": column_value,
                   "count": count,
                   "downtime": total_downtime
               })

           metric=""
           metric_downtime=""

           for item in result_data:
               metric += "incidents.seen,config_id=\"" + item['entityId'] + "\"" + ",year="+ str(item['year']) + ",month=" + str(item['month']) + ",problem_title=\"" + str(item['column_value']) + "\",mgmt_zone=\"" + str(item['zone']) + "\" " + str(item['count']) + "\n"
               metric_downtime += "downtime,config_id=\"" + item['entityId'] + "\"" + ",year="+ str(item['year']) + ",month=" + str(item['month']) + ",problem_title=\"" + str(item['column_value']) + "\",downtime=" + str(item['downtime']) + ",mgmt_zone=\"" + str(item['zone']) + "\" " + str(item['downtime']) + "\n"

           self.dtApiIngestMetrics(self.INGEST_METRICS,metric, endpoint_config)

           self.logger.info(metric_downtime)
           self.dtApiIngestMetrics(self.INGEST_METRICS,metric_downtime, endpoint_config)

       except Exception as e:
          self.logger.exception("Exception encountered slice_and_dice_problem_trend" + str(e))

       finally:
          self.logger.info("Successful execution: slice_and_dice_problem_trend")

    # ********************************************************************************************************
    #        Function to populate problem metrics
    # ********************************************************************************************************
    def populate_problem_data(self, entityID, data, problems_mgmt_zone, ag_endpoint, endpoint_config):
        #try:
          self.logger.info("In populate_problem_data")
          self.logger.info(endpoint_config) 

          mean_rsp_time=[]
          median_rsp_time = 0
          total_prb_resolved = 0
          total_number_of_prb = 0 
          csv_data = self.initialize_csv_header()
          csv_data_list = []
          #Data that we will dump to log file so it can be retrieved (where logV2 is disabled)
          logger_csv_data = ""
          logger_csv_data="config_id,Endpoint Name,status,management.zone,Problem ID,Problem Link,starttime,endtime,problem.title,impact.level,severity.level,RCA or no RCA,mttr\n"

          #Value across the endpoint (and not limited to management zone)
          total_incident_reported = 0
          incidents_with_rca = 0
          incidents_wo_rca = 0
          total_mttr_rca = []
          total_mttr_wo_rca = []
          for i in range(len(data)):
            start_time = data[i]["startTime"]
            end_time = data[i]["endTime"]

            if end_time != -1:
              total_prb_resolved = total_prb_resolved + 1
              resolution_time = end_time - start_time
              
              if int(resolution_time) >= int(endpoint_config["problem_to_incident_duration"]): 
                total_incident_reported = total_incident_reported + 1
                #Management Zone
                key = ""

                try:
                  zones = data[i]['managementZones']

                  if len(zones) != 0:
                    for zone in zones:
                      key = key + zone['name'] + ","
                    key = key[:-1]
                  else:
                    key = "No management zone"

                except KeyError:
                  key = "No management zone"

                try:
                    problems_mgmt_zone[key].problems = problems_mgmt_zone[key].problems + 1

                except KeyError:
                    obj = self.problem_mgmt_zone()
                    obj.problems=1
                    obj.rootCause=0
                    obj.application=0
                    obj.service=0
                    obj.infrastructure=0
                    obj.error=0
                    obj.custom=0
                    obj.availability=0
                    obj.performance=0
                    obj.resource=0
                    obj.mttr_rca=0
                    obj.mgmt_zone=key
                    obj.mttr_wo_rca=-1
                    obj.mttr_rca_list=[]
                    obj.mttr_wo_rca_list=[]
                    problems_mgmt_zone[key]=obj
                
                if (data[i]["rootCauseEntity"]):
                   incidents_with_rca = incidents_with_rca + 1
                   total_mttr_rca.append(resolution_time)

                   problems_mgmt_zone[key].rootCause = problems_mgmt_zone[key].rootCause + 1
                   problems_mgmt_zone[key].mttr_rca_list.append(resolution_time)

                   #Check the length of csv_data since we have a limitation of allowing a string of only 5000 characters
                   if (csv_data.count('\n') >= 400):
                     csv_data_list.append(csv_data)
                     csv_data = self.initialize_csv_header()

                   csv_data = csv_data + entityID + "," + entityID + ",INFO,\"" + key + "\"," + data[i]["displayId"] + "," + endpoint_config["url"][:-7] + "#problems/problemdetails;gf=all;pid=" + data[i]["problemId"] + "," + data[i]["title"] + "," + data[i]["impactLevel"] + "," + data[i]["severityLevel"] + ",rca," + str(resolution_time/3600000) + "\n"
                   logger_csv_data = logger_csv_data + entityID + "," + entityID + ",INFO,\"" + key + "\"," + data[i]["displayId"] + "," + endpoint_config["url"][:-7] + "#problems/problemdetails;gf=all;pid=" + data[i]["problemId"] + "," + str(int(start_time/1000)) + "," + str(end_time/1000) + "," + data[i]["title"] + "," + data[i]["impactLevel"] + "," + data[i]["severityLevel"] + ",rca," + str(resolution_time/3600000) + "\n"

                else:   
                   total_mttr_wo_rca.append(resolution_time)
                   incidents_wo_rca = incidents_wo_rca + 1 
                   problems_mgmt_zone[key].mttr_wo_rca_list.append(resolution_time)

                   #Check the length of csv_data since we have a limitation of allowing a string of only 5000 characters
                   if (csv_data.count('\n') >= 400):
                     csv_data_list.append(csv_data)
                     csv_data = self.initialize_csv_header()

                   csv_data = csv_data + entityID + "," + entityID + ",INFO,\"" +  key + "\"," + data[i]["displayId"] + "," + endpoint_config["url"][:-7] + "#problems/problemdetails;gf=all;pid=" + data[i]["problemId"] + "," + data[i]["title"] + "," + data[i]["impactLevel"] + "," + data[i]["severityLevel"] + ",no_rca,"+str(resolution_time/3600000)+"\n"
                   logger_csv_data = logger_csv_data + entityID + "," + entityID + ",INFO,\"" +  key + "\"," + data[i]["displayId"] + "," + endpoint_config["url"][:-7] + "#problems/problemdetails;gf=all;pid=" + data[i]["problemId"] + "," + str(int(start_time/1000)) + "," + str(end_time/1000) + "," + data[i]["title"] + "," + data[i]["impactLevel"] + "," + data[i]["severityLevel"] + ",no_rca,"+str(resolution_time/3600000)+"\n"
                mean_rsp_time.append(resolution_time)
                severity = data[i]["severityLevel"]

                if severity == "AVAILABILITY":
                  problems_mgmt_zone[key].availability += 1

                elif severity == "PERFORMANCE":
                  problems_mgmt_zone[key].performance += 1

                elif severity == "ERROR":
                 problems_mgmt_zone[key].error += 1

                elif severity == "RESOURCE_CONTENTION":
                  problems_mgmt_zone[key].resource += 1

                elif severity == "CUSTOM_ALERT":
                  problems_mgmt_zone[key].custom += 1

                impact_level = data[i]["impactLevel"]
                if impact_level == "SERVICES":
                  problems_mgmt_zone[key].service += 1

                elif impact_level == "APPLICATION":
                  problems_mgmt_zone[key].application += 1

                elif impact_level == "INFRASTRUCTURE":
                 problems_mgmt_zone[key].infrastructure += 1

          for key in problems_mgmt_zone.keys():
              metric = ""
              self.logger.info("populating metrics for -> {}".format(key))
              #Find the median response time for each mgmt_zone and convert it to minutes (from microseconds)
              try:
                problems_mgmt_zone[key].mttr_rca = ((sum(problems_mgmt_zone[key].mttr_rca_list)/len(problems_mgmt_zone[key].mttr_rca_list)))/60000
              except ZeroDivisionError:
                problems_mgmt_zone[key].mttr_rca = -1

              #Find the median response time for each mgmt_zone and convert it to minutes (from microseconds)
              try:
                problems_mgmt_zone[key].mttr_wo_rca = ((sum(problems_mgmt_zone[key].mttr_wo_rca_list)/len(problems_mgmt_zone[key].mttr_wo_rca_list)))/60000
              except ZeroDivisionError:
                problems_mgmt_zone[key].mttr_wo_rca = -1 

              # Push management zone metric only if config is set to yes 
              if endpoint_config["get_problem_data_mgmt_zone"] == "Yes":
                metric += "total_reported_problems,mgmt_zone=\"" + key + "\"" + ",config_id=" + entityID + " " + str(problems_mgmt_zone[key].problems) + "\n"
                metric += "root_cause,mgmt_zone=\"" + key + "\"" + ",config_id=" + entityID + " " + str(problems_mgmt_zone[key].rootCause) + "\n"
                metric += "reported_availability_problems,mgmt_zone=\"" + key + "\"" + ",config_id=" + entityID + " " + str(problems_mgmt_zone[key].availability) + "\n"
                metric += "reported_performance_problems,mgmt_zone=\"" + key + "\"" + ",config_id=" + entityID + " " + str(problems_mgmt_zone[key].performance) + "\n"
                metric += "reported_resource_problems,mgmt_zone=\"" + key + "\"" + ",config_id=" + entityID + " " + str(problems_mgmt_zone[key].resource) + "\n"
                metric += "reported_custom_problems,mgmt_zone=\"" + key + "\"" + ",config_id=" + entityID + " " + str(problems_mgmt_zone[key].custom) + "\n"
                metric += "reported_service_problems,mgmt_zone=\"" + key + "\"" + ",config_id=" + entityID + " " + str(problems_mgmt_zone[key].service) + "\n"
                metric += "reported_infra_problems,mgmt_zone=\"" + key + "\"" + ",config_id=" + entityID + " " + str(problems_mgmt_zone[key].infrastructure) + "\n"
                metric += "reported_application_problems,mgmt_zone=\"" + key + "\"" + ",config_id=" + entityID + " " + str(problems_mgmt_zone[key].application) + "\n"
                metric += "reported_error_problems,mgmt_zone=\"" + key + "\"" + ",config_id=" + entityID + " " + str(problems_mgmt_zone[key].error) + "\n"
                metric += "mttr_with_rca,mgmt_zone=\"" + key + "\"" + ",config_id=" + entityID + " " + str(problems_mgmt_zone[key].mttr_rca) + "\n"
                metric += "mttr_wo_rca,mgmt_zone=\"" + key + "\"" + ",config_id=" + entityID + " " + str(problems_mgmt_zone[key].mttr_wo_rca) + "\n"

                self.dtApiIngestMetrics(self.INGEST_METRICS, metric, endpoint_config)

          if (endpoint_config["get_problem_data_mgmt_zone"] == "Yes"):
              metric = ""
              import time
              now = time.time()

              #Insert epoch time at the time of last record being inserted
              metric += "record_insertion_time" + ",config_id=" + entityID + " " + str(now) + "\n"
             
              mttr_rca = 0
              mttr_wo_rca = 0
              #Total mttr for the problems with rca 
              try:
                mttr_rca = ((sum(total_mttr_rca)))/60000
              except ZeroDivisionError:
                mttr_rca = -1

              #Total mttr for the problems with rca 
              try:
                mttr_wo_rca = ((sum(total_mttr_wo_rca)))/60000
              except ZeroDivisionError:
                mttr_wo_rca = -1

              #Insert the total incidents reported 
              metric += "total_incident_reported" + ",config_id=" + entityID + " " + str(total_incident_reported) + "\n"
              metric += "total_incidents_with_rca" + ",config_id=" + entityID + " " + str(incidents_with_rca) + "\n"
              metric += "total_incidents_wo_rca" + ",config_id=" + entityID + " " + str(incidents_wo_rca) + "\n"
              metric += "total_mttr_rca" + ",config_id=" + entityID + " " + str(mttr_rca) + "\n"
              metric += "total_mttr_wo_rca" + ",config_id=" + entityID + " " + str(mttr_wo_rca) + "\n"

              self.dtApiIngestMetrics(self.INGEST_METRICS,metric, endpoint_config)
              
              prb_report_date = endpoint_config["get_generate_report"]
              #Once data is pushed, set next collection interval accordingly
              if prb_report_date == "Last 30 days":
                problem_time_interval = (30*1440) - 1
              elif prb_report_date == "Last 60 days":
                problem_time_interval = (60*1440) - 1
              elif prb_report_date == "Last 90 days":
                problem_time_interval  = (90*1440) - 1
              else:
                problem_time_interval  = (365*1440) - 1
              
          #Find the median response time and convert it to minutes (from microseconds)
          try:
            median_rsp_time = ((sum(mean_rsp_time)/len(mean_rsp_time)))/60000
          except ZeroDivisionError:
            median_rsp_time = 0

#        except Exception as e:
#          self.logger.exception("Exception encountered populate_problem_data" + str(e))

#        finally:
#         self.logger.info("Successful execution: populate_problem_data")
#          self.logger.info("Pushing the problem data in logs")
#          self.logger.info(logger_csv_data)

          self.slice_and_dice_problem_trend(self.logger,logger_csv_data, endpoint_config)

          # Get the endpoint from ag_enpoint
          if ag_endpoint != "":
            #Push the latest csv_data   
            csv_data_list.append(csv_data)  

            for csv_data in csv_data_list:
              reader = csv.DictReader(io.StringIO(csv_data))
              json_data = json.dumps(list(reader))

              query = ag_endpoint + "/logs/ingest"
              self.dtApiV2PushLogs(query,json_data, endpoint_config)

          return median_rsp_time, problems_mgmt_zone


    # *******************************************************************************    
    #           Function to post data using API v2 endpoint
    # *******************************************************************************    
              
    def dtApiV2PushLogs(self, query, payload, endpoint_config):
      try:    
        self.logger.info("In dtApiV2PushLogs")
                
        data = {}
        conf_password=endpoint_config["conftoken"]
        post_param = {'Accept':'application/json','Content-Type':'application/json; charset=utf-8', 'Authorization':'Api-Token {}'.format(conf_password)}
        
        #populate_data = requests.post(query, headers = post_param, data = payload, verify=False)
        #self.logger.info(query)
        #self.logger.info(populate_data.status_code)
        #        
        #if populate_data.status_code == 401:
        #  msg = "Auth Error"
        #  self.logger.exception("Auth Error dtApiV2PushLogs: ")
              
      except Exception as e:
        self.logger.exception("Exception in dtApiV2PushLogs" + str(e))
                
      finally:  
        self.logger.info("Succesfully completed dtApiV2PushLogs")


    # *******************************************************************************
    #           Function for configuration API for publish tenant
    # *******************************************************************************
    def dtConfApiv1(self, endpoint):
      try:
        self.logger.info("In dtConfApiv1")
        data = {}

        url = self.confurl
        url = url.replace("v1","config/v1")
        url = url.replace("v2","config/v1")

        query = str(url) + endpoint
        get_param = {'Accept':'application/json', 'Authorization':'Api-Token {}'.format(self.conf_password)}
        populate_data = requests.get(query, headers = get_param, verify=False)
        self.logger.info(query)
        self.logger.info(populate_data.status_code)

        if populate_data.status_code >=200 and populate_data.status_code <= 400:
          data = populate_data.json()

        elif populate_data.status_code == 401:
          msg = "Auth Error"

      except Exception as e:
        self.logger.exception("Exception encountered in dtConfApiv1 ", str(e))

      finally:
        self.logger.info("Execution completed dtConfApiv1 ")
        return data

    # *******************************************************************************    
    #           Function for configuration API for pulling data
    # *******************************************************************************    
    def dtConfApi(self, endpoint):
      try:
        self.logger.info("In dtConfApi")  
        data = {}

        url = self.url
        url = url.replace("v1","config/v1")

        query = str(url) + endpoint
        get_param = {'Accept':'application/json', 'Authorization':'Api-Token {}'.format(self.password)}
        populate_data = requests.get(query, headers = get_param, verify=False)
        self.logger.info(query)

        if populate_data.status_code >=200 and populate_data.status_code <= 400:
          data = populate_data.json()
        elif populate_data.status_code == 401:
          msg = "Auth Error"

      except Exception as e:
        self.logger.exception("Exception encountered in dtConfApi ", str(e))  

      finally:
        self.logger.info("Execution completed dtConfApi ")  
        return data

def main():
    ExtensionImpl().run()

if __name__ == '__main__':
    main()
