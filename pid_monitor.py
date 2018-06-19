import psutil
import logging
import time
start = time.time()
PERIOD_OF_TIME = 120 # 
while True:
    plist = []
    if time.time() > start + PERIOD_OF_TIME : break
    try:
        
        plist = psutil.pids()
        for x in plist:
            p =  psutil.Process(x)
            cmdline = p.cmdline()
            print(cmdline)
            
            
                
    except psutil.AccessDenied:
        pass
    except OSError:
        pass
    except psutil.NoSuchProcess:
        continue
  
