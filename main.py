from pymongo import MongoClient
from zabbix import Zabbix
from datetime import datetime


cliente = MongoClient('localhost', 27017)
ids = cliente['IDS']
inat = ids['inatividades']
camCol = ids["cameras"]

collist = ids.list_collection_names()
print(collist)
"""
x = camCol.delete_many({})
while( x.acknowledged == False  ):
    x = camCol.delete_many({})
    print("Unsuccessfully trying to delete cameras collection")
print("Camera collection successfully deleted", x.deleted_count,"documents deleted.")
"""

"""
camera -> nome, log{message, date}, history[{date, value}]
user -> login, passwords, devices[], iqi
ids => calcula na hora

activity => camera * value * date
camera => camera * log 
"""
#*********************************************************************************************************************************

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
        """
        cameraName = {}
        cameraName["name"] = host["name"]
        x = camCol.insert_one(cameraName)
        while x.acknowledged == False:
            print("FAIL TO SAVE", cameraNames)
            x = camCol.insert_one(cameraNames)
        """
#print("Cameras Saved On database")
item = Zabbix.getItemsDataByHostIdsAndItemsName(hostIds, "Ping")

dataBaseCounter = 0
dataCounter = 0
oneItem = []
itemDict = {}

for each in item:
    for camera in cameras:
        if camera["hostid"] == each["hostid"]:
            camera["itemid"] = each["itemid"]
            itemDict[  each["itemid"]  ]  =  camera["name"] 
    
today = datetime.now()

print(today)

year = today.year
month = today.month
day = today.day
init = int(datetime(year, month, 1).timestamp())
end = int(datetime(year, month, day).timestamp() -1)

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
endthispart = datetime.now()
timeforamonth = endthispart - today
print(timeforamonth)

for each in item:
    oneItem = each["itemid"]
    for y in reversed(range(2017, 2020)):
        for m in reversed(range(1, 13)):
            if m != 12:
                m2 = m +1
                y2 = y
            else:
                m2 = 1
                y2 = y + 1
            if y == year and m == month:
                continue
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
print("Transform " + str(dataCounter) + " results into " + str(dataBaseCounter) + " results\n")

end = datetime.now()
timexecuting = end - today
print(timexecuting)