from pymongo import MongoClient
from zabbix import Zabbix
from datetime import datetime
import time

cliente = MongoClient('localhost', 27017)
ids = cliente['IDS']
inat = ids['inatividades']
camCol = ids["cameras"]

collist = ids.list_collection_names()
print(collist)

def updateYesterdayData(REGIAO, HOUR):
  now = datetime.now()
  currentHour = now.hour
  cameraPromblems = {}
  if currentHour == HOUR:
    print("Saving", REGIAO)
    for x in camCol.find():
        if(x.get("problem")): 
            cameraPromblems[x["name"]] = x["problem"]
    deletequery = { "regiao": REGIAO }
    x = camCol.delete_many(deletequery)
    while( x.acknowledged == False  ):
        x = camCol.delete_many({})
        print("Unsuccessfully trying to delete cameras collection")
    print("Camera collection successfully deleted", x.deleted_count,"documents deleted.")
    
    result = Zabbix.getProject(REGIAO)  # Returns the project's serviceid and its dependencies' serviceids 
    dependencies = result["dependencies"]

    serviceids = []
    for device in dependencies:
        serviceids.append(device["serviceid"])
    result = Zabbix.getServiceDataByServiceIds(serviceids) # Returns the name of the cameras that has these serviceids

    hostsNames = []
    for device in result:
        hostsNames.append(device["name"])
    hosts = Zabbix.getHostsDataByHostNames(hostsNames)

    hostIds = []
    cameras = []
    for host in hosts:
        if host["status"] == '0':
            camera = {}
            camera["name"] = host["name"]
            camera["hostid"] = host["hostid"]
            cameras.append(camera)
            hostIds.append(host["hostid"])
            cameraName = {}
            cameraName["name"] = host["name"]
            cameraName["regiao"] = REGIAO
            if(host["name"] in cameraPromblems):
                cameraName["problem"] = cameraPromblems[host["name"]]
            x = camCol.insert_one(cameraName)
            while x.acknowledged == False:
                print("FAIL TO SAVE", cameraNames)
                x = camCol.insert_one(cameraNames)
    print("Cameras Saved On database")
    item = Zabbix.getItemsDataByHostIdsAndItemsName(hostIds, "Ping")

    itemDict = {}

    for each in item:
        for camera in cameras:
            if camera["hostid"] == each["hostid"]:
                camera["itemid"] = each["itemid"]
                itemDict[  each["itemid"]  ]  =  camera["name"]    
    
    # Start saving history
    
    today = datetime.now()
    print(today)
    dataBaseCounter = 0
    year = now.year
    month = now.month
    day = now.day
    end = datetime(year, month, day).timestamp() - 1
    init = end - 86400 + 1
    print( "From", datetime.fromtimestamp(init)   )
    print( "To", datetime.fromtimestamp(end)   )   
    for each in item:
      oneItem = each["itemid"]
      result = Zabbix.getHistoryByItemId(oneItem, init, end)
      length = len(result)
      if result:
          for data in result:
              dt = data
              value = data["value"]
              databasefile = {}
              dataBaseCounter = dataBaseCounter + 1
              databasefile["camera"] = itemDict[oneItem]
              databasefile["date"] = int(data["clock"])
              databasefile["value"] = int(  float(data["value"])   )
              x = inat.insert_one(databasefile)
              while x.acknowledged == False:
                  print("FAIL TO SAVE",databasefile)
                  x = inat.insert_one(databasefile)
          print(itemDict[oneItem], datetime.fromtimestamp(init), datetime.fromtimestamp(end), "** " +  str(length) + " results")
      else:
          print(itemDict[oneItem], datetime.fromtimestamp(init), datetime.fromtimestamp(end), "There is no result for that")
    
    print(dataBaseCounter, "files saved on mongo")
    endOfSave = datetime.now()
    timexecuting = endOfSave - today
    print(timexecuting)
    
    
while(1):
  updateYesterdayData("SERGIPE", 2)
  updateYesterdayData("TECARMO", 3)
  updateYesterdayData("RUA ACRE", 4)
  now = datetime.now()
  print(now, "Waiting Update Time")
  time.sleep(60*60) #60*60

