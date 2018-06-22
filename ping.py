from platform   import system as system_name  # Returns the system/OS name
from subprocess import call   as system_call  # Execute a shell command

def ping(host):
    """
    Returns True if host (str) responds to a ping request.
    Remember that a host may not respond to a ping (ICMP) request even if the host name is valid.
    """

    # Ping command count option as function of OS
    param = '-n' if system_name().lower()=='windows' else '-c'

    # Building the command. Ex: "ping -c 1 google.com"
    command = ['ping', param, '1', host]

    # Pinging
    return system_call(command) == 0
def validate():
    print(ping("google.com"))

if __name__=='__main__':
    validate()
    


    
