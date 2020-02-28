import json
import time
import urllib3
import boto3
import datetime
from datetime import date, timedelta


def convert_waze_json_to_cml_json(url,bucket,filename,waze_icon,cml_icon): #Current waze incidents
    '''
    transforms waze json into specfied cml json
    
    where:
        url is waze proved request endpoint
        bucket is S3 output location
        filename is the key
        waze_icon the custom path for waze icon
        cml_icon the custom path for cml icon in header
    
    json return sample (alert):
      "country": "AS",
      "nThumbsUp": 0,
      "city": "South Ripley",
      "reportRating": 0,
      "confidence": 0,
      "reliability": 6,
      "type": "ROAD_CLOSED",
      "uuid": "65293e8f-9ff5-3655-ad02-3befe926992f",
      "magvar": 0,
      "subtype": "ROAD_CLOSED_EVENT",
      "street": "Ripley Rd",
      "reportDescription": "",
      "location": {
        "x": 152.803307,
        "y": -27.69669
      },
      "pubMillis       
        
    '''
    
    #Waze up session and payload to waze
    http = urllib3.PoolManager()
    try: 
        response = http.request('GET',url)
        waze_status = json.loads(response.data.decode('utf-8'))
    except:
        response = "failed!"
    
    #header
    #tmr_waze_json={"feed": []}
    tmr_waze_json={"feed": {"layer":[{"name":"CML - Waze Alerts","icon":cml_icon,"features":[]}]}}

    #processing.....
    #ToDo - 1)Exceptions rules here
    for alerts in waze_status["alerts"]:
        keys_input = waze_status["alerts"]
        points_list = []
        locations = alerts.get("location","")
        if locations == "":
            continue
        else:
            points_list.extend([locations["x"], locations["y"]])

        type_transformed  = alerts["type"]
        if type_transformed == "WEATHERHAZARD":
            type_transformed = "Hazard"
        elif type_transformed == "ROAD_CLOSED": 
            type_transformed = "Road closure"
        elif type_transformed == "JAM": 
            type_transformed = "Congestion"
        elif type_transformed == "ACCIDENT": 
            type_transformed = "Crash"
        else:
            print("not found type - "+subtype_transformed)

        subtype_transformed  = alerts["subtype"]
        if subtype_transformed == "HAZARD_ON_SHOULDER_CAR_STOPPED":
            subtype_transformed = "stationary vehicle on shoulder"
        elif subtype_transformed == "ROAD_CLOSED_EVENT": 
            subtype_transformed = "Road closure"
        elif subtype_transformed == "HAZARD_ON_ROAD_OBJECT": 
            subtype_transformed = "object on road"
        elif subtype_transformed == "JAM_HEAVY_TRAFFIC": 
            subtype_transformed = "heavy"
        elif subtype_transformed == "JAM_MODERATE_TRAFFIC": 
            subtype_transformed = "moderate"
        elif subtype_transformed == "JAM_STAND_STILL_TRAFFIC": 
            subtype_transformed = "very heavy"               
        elif subtype_transformed == "HAZARD_ON_ROAD_CAR_STOPPED": 
            subtype_transformed = "stationary vehicle on road"
        elif subtype_transformed == "HAZARD_ON_ROAD_POT_HOLE": 
            subtype_transformed = "pot hole"
        elif subtype_transformed == "ACCIDENT_MINOR": 
            subtype_transformed = "minor crash"
        elif subtype_transformed == "ACCIDENT_MAJOR":
             subtype_transformed = "major crash"           
        elif subtype_transformed == "HAZARD_ON_ROAD_CONSTRUCTION": 
            subtype_transformed = "roadworks"
        elif subtype_transformed == "HAZARD_ON_ROAD": 
            subtype_transformed = "hazard on road"    
        elif subtype_transformed == "HAZARD_ON_ROAD_ROAD_KILL": 
            subtype_transformed = "roadkill on road"           
        elif subtype_transformed == "": 
            subtype_transformed = "unknown"
        else:
            print("not found subtype-"+subtype_transformed)

        
        properties_dict = {
            "name" : "Waze - " + type_transformed +" "+ subtype_transformed +" "+str(alerts.get("city","")),
            "icon" : waze_icon,
            "tooltip" : "Waze - " + type_transformed +" "+ subtype_transformed +" "+str(alerts.get("city",""))+" "+str(alerts.get("street","")),
        	"featurewindow" : {"href": "http://#"},
        	"menu": [{"title": "","href": ""}]
             }
        
        alert_dict = {"type": "Feature",
                     "id": alerts.get("uuid",""),
                     "geometry": { "type":"Point", "coordinates":points_list},
                     "properties" : properties_dict
        }
    
        tmr_waze_json["feed"]["layer"][0]["features"].append(alert_dict)
  

    #ouput trasformed json
    dict_to_json = json.dumps(tmr_waze_json)
    s3 = boto3.client('s3') 
    s3.put_object(Body=dict_to_json, Bucket=bucket,Key=filename)
    return

    #raw i.e waze-20200225-1705.json - for QAing
    dt = datetime.datetime.utcnow() + datetime.timedelta(hours=10)
    today = str(datetime.datetime.strftime(dt,"%Y%m%d"))  #string yyyymmdd
    current_time= str(datetime.datetime.strftime(dt,"%H%M"))
    waze_json_input = 'waze-'+today+'-'+current_time+'.json'
    waze_json = json.dumps(waze_status)     
    s3.put_object(Body=waze_json, Bucket=bucket,Key=waze_json_input)
    return

def lambda_handler(event, context):
    
    #initialise parameters
    waze_url =''
    
    s3_bucket_output = 'streams-cml'
    waze_json_output = 'cml-test-5.json' 
    waze_icons1 = '/icons/dot_orange.png'
    waze_icons2 = '/icons/dot_red.png'
    waze_icons3 = '/icons/dot_yellow.png'
    cml_icon = '/icons/waze_icon.png'
    
    try:
        convert_waze_json_to_cml_json(waze_url,s3_bucket_output,waze_json_output,waze_icons2,cml_icon)
    except:
        print("failed waze conversion")
        
    print("Compeleted")
        
    return 