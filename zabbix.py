from requests import request
import json
import sys


class Zabbix:
    #******************GLOBAL VARIABLES DEFINITION******************
    global url
    global headers
    global payload

    url = "http://saffira.aton.com.br/zabbix/api_jsonrpc.php"

    headers = {}
    headers['Content-Type'] = 'application/json'

    payload = {}
    payload['jsonrpc'] = "2.0"
    payload['id'] = "1" 

    #****************************************************************


    #*******************AUTHENTICATION METHOD************************
    global hasError
    def hasError(error):
        if (error):
            print("ERROR: " + error['message'])
            sys.exit()
        else:
            return False

    global getLogin
    def getLogin():
        arg = sys.argv
        maxLen = len(arg)
        login = {}
        for i, val in enumerate(arg):
            if val == '-h' or val == '--help':
                print("\n *** ENTER -u [ZABBIX USER] -p [ZABBIX PASSWORD] *** \n")
                sys.exit() 
            elif val == '-u' or val == '--user':
                if i < (maxLen-1):
                    login['user'] = arg[i+1]
            elif val == '-p' or val == '--password':
                if i < (maxLen-1):
                    login['password'] = arg[i+1]
        
        if len(arg) < 2 or not login.get('user') or not login.get('password'):
            print("\n *** TRY -h *** \n")
            sys.exit()
        else:
            return login

    def authenticate():
        payload['method'] = "user.login"
        login = getLogin()
        payload['params'] = login
        while(1):
            try:
                response = request("POST", url, headers= headers, data = json.dumps(payload), timeout = 300 )
                break
            except:
                print("Exception on authenticate")
                continue
        responseObj = json.loads(response.text)
        if not hasError(responseObj.get('error')):
            payload['auth'] = responseObj['result']
            print("Zabbix authentication Done!")
    authenticate()
    #****************************************************************



    #****************************************************************
    def getProject(projectName):
        payload['method'] = "service.get"
        
        params = {}
        params['output'] = "extend"
        params['selectDependencies'] = "extend"
        
        search = {}
        search['name'] = projectName        
        params['search'] = search

        payload['params'] = params

        while(1):
            try:
                response = request("POST", url, headers= headers, data = json.dumps(payload), timeout = 300 )
                break
            except:
                print("Exception on getProject")
                continue
        responseObj = json.loads(response.text)
        if not hasError(responseObj.get('error')):
            return responseObj['result'][0]
    #****************************************************************

    def getServiceDataByServiceIds(serviceids):
        payload['method'] = "service.get"

        params = {}
        params['output'] = "extend"
        params['selectDependencies'] = "extend"
        params["serviceids"] = serviceids

        payload['params'] = params

        while(1):
            try:
                response = request("POST", url, headers= headers, data = json.dumps(payload), timeout = 300 )
                break
            except:
                print("Exception on getServiceDataByServiceIds")
                continue
        responseObj = json.loads(response.text)
        if not hasError(responseObj.get('error')):
            return responseObj['result']

    def getHostsDataByHostNames(hostname):
        payload['method'] = "host.get"
        
        _filter = {}
        _filter["host"] = hostname

        params = {}
        params["filter"] = _filter

        payload["params"] = params

        while(1):
            try:
                response = request("POST", url, headers= headers, data = json.dumps(payload), timeout = 300 )
                break
            except:
                print("Exception on getHostsDataByHostNames")
                continue
        responseObj = json.loads(response.text)
        if not hasError(responseObj.get('error')):
            return responseObj['result']

    def getItemsDataByHostIdsAndItemsName(hostids, name):
        payload['method'] = "item.get"

        _filter = {}
        _filter["hostid"] = hostids
        _filter["name"] = name

        params = {}
        params["output"] = "extend"
        params["filter"] = _filter

        payload["params"] = params

        while(1):
            try:
                response = request("POST", url, headers= headers, data = json.dumps(payload), timeout = 300 )
                break
            except:
                print("Exception on getItemsDataByHostIdsAndItemsName")
                continue
        responseObj = json.loads(response.text)
        if not hasError(responseObj.get('error')):
            return responseObj['result']

    def getHistoryByItemId(itemid, init= False, end= False, order = "DESC" ):
        payload['method'] = "history.get"

        _filter = {}
        _filter["value"] = "0.0000"

        params = {}
        params["output"] = "extend"
        params["history"] = "0"
        params["itemids"] = itemid
        params["filter"] = _filter
        #params["sortfield"] = "clock"
        """
        if not order:
            params["sortorder"] = "DESC"
        else:
            params["sortorder"] = order
        """

        if init and end:
            params["time_from"] = init
            params["time_till"] = end
        else:
            params["limit"] = "5000"
      
        payload["params"] = params

        while(1):
            try:
                response = request("POST", url, headers= headers, data = json.dumps(payload), timeout = 300 )  
                break
            except:
                print("Exception on getHistoryByItemId")
                continue
        responseObj = json.loads(response.text)
        if not hasError(responseObj.get('error')):
            return responseObj['result']



