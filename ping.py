from platform   import system as system_name  # Returns the system/OS name
from subprocess import call   as system_call  # Execute a shell command
import json
from pprint import pprint
import requests
def ping(host):
    """
    Returns True if host (str) responds to a ping request.
    Remember that a host may not respond to a ping (ICMP) request even if the host name is valid.
    """

    # Ping command count option as function of OS
    param = '-n' if system_name().lower()=='windows' else '-c'

    # Building the command. Ex: "ping -c 5 google.com"
    command = ['ping', param, '5', host]

    # Pinging
    return system_call(command) == 0



def validate():

    with open('servers.json') as f:
        
        data = json.load(f)
        servers = data["servers"]["sjc_prod"]
        for i in servers:
            if (ping(i)==False):
                message = "server " + i + " is DEAD!"
                trigger_incident(message, i) #i is hostname


ROUTING_KEY = "44d8052aff534f138f7c3170ed7584bc"
#qlik dpi pager-duty room 44d8052aff534f138f7c3170ed7584bc
# ENTER EVENTS V2 API INTEGRATION KEY HERE


def trigger_incident(message, hostname):
    # Triggers a PagerDuty incident without a previously generated incident key
    # Uses Events V2 API - documentation: https://v2.developer.pagerduty.com/docs/send-an-event-events-api-v2

    header = {
        "Content-Type": "application/json"
    }

    payload = {  # Payload is built with the least amount of fields required to trigger an incident
        "routing_key": ROUTING_KEY,
        "event_action": "trigger",
        "payload": {

           "summary": message,
            "source": hostname,
            "severity": "critical"
        }
    }

    response = requests.post('https://events.pagerduty.com/v2/enqueue',
                             data=json.dumps(payload),
                             headers=header)

    if response.json()["status"] == "success":
        print ('Incident created with with dedup key (also known as incident / alert key) of ' + '"' + response.json()[
            'dedup_key'] + '"')
    else:
        print (response.text)  # print error message if not successful




if __name__=='__main__':
    validate()
    
        
        


                      
    


    
