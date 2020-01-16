from pymongo import MongoClient
from zabbix import Zabbix
from datetime import datetime


cliente = MongoClient('localhost', 27017)
ids = cliente['IDS']
inat = ids['inatividades']
camCol = ids["cameras"]

result = Zabbix.getProject('SERGIPE')  # Returns the project's serviceid and its dependencies' serviceids 
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
item = Zabbix.getItemsDataByHostIdsAndItemsName(hostIds, "Ping")

dataBaseCounter = 0
dataCounter = 0
oneItem = []
itemDict = {}
cameraDict = {}

for each in item:
    for camera in cameras:
        if camera["hostid"] == each["hostid"]:
            camera["itemid"] = each["itemid"]
            itemDict[  each["itemid"]  ]  =  camera["name"] 
            cameraDict[ camera["name"]  ] =  each["itemid"] 
    
today = datetime.now()
continueFlag = False
personalisedCameraName = "CAM57_OFICINA DE SOLDA"

print(today)

"""       
# *** Method #1 - Get certain cameras' data of defined time ***
 
year = 2019
month = 3

init = int(datetime(year, month, 1).timestamp())
end = int(datetime(year, month+1, 1).timestamp() -1)

print(datetime.fromtimestamp(init), datetime.fromtimestamp(end))

oneItem = cameraDict[personalisedCameraName]
print(oneItem)
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
"""

# *** Method #2 - Get only part of cameras' data ***
for each in item:
    oneItem = each["itemid"]
    if(cameraDict[personalisedCameraName] == oneItem):
        continueFlag = True
        continue
    if(continueFlag):
        for y in reversed(range(2017, 2020)):
            for m in reversed(range(1, 13)):
                if m != 12:
                    m2 = m +1
                    y2 = y
                else:
                    m2 = 1
                    y2 = y + 1
                init = int(datetime(y, m, 1).timestamp())
                end = int(datetime(y2, m2, 1).timestamp() -1)
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

end = datetime.now()
timexecuting = end - today
print(timexecuting)