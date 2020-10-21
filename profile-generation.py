import csv
import sys
import random
import mysql.connector
from random import randint
import datetime
import time
import threading
import signal

threads = []

def generate_employee_data(thread_num, rootuser, ipaddress, numbatch):
    last_no_file = "./profile/last_no.csv"
    with open("./profile/firstname.csv", mode='r') as firstnamefile:
        next(firstnamefile)
        csv_reader1 = csv.reader(firstnamefile, delimiter=',')
        firstnamelist = list(csv_reader1)
    with open("./profile/lastname.csv", mode='r') as lastnamefile:
        next(lastnamefile)
        csv_reader2 = csv.reader(lastnamefile, delimiter=',')
        lastnamelist = list(csv_reader2)
    with open('./profile/conf.csv', 'rt') as csv_file:
        next(csv_file)
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            print row
            total_rows = int(row[0])
            mydb = mysql.connector.connect(user=rootuser, password='27Network',
                                           host=ipaddress, database='Cisco')
            status = ""
            batchsize = 0
            cursor = mydb.cursor()
            for e in range(total_rows):
                batchsize += 1
                random_firstname = random.choice(firstnamelist)
                employee_fname = str((random_firstname[1])).upper()
                employee_gender = (random_firstname[3])
                random_lastname = random.choice(lastnamelist)
                employee_ln = (random_lastname[0]).capitalize()
                status_num = random.randint(0, 1)
                age = random.randint(44, 70)
                emp_hire_date = datetime.date(randint(1993, 2019), randint(1, 12), randint(1, 28))
                if (emp_hire_date.year < 2001):
                    age = random.randint(41, 70)
                if ((emp_hire_date.year >= 2001) and (emp_hire_date.year <= 2008)):
                    age = random.randint(30, 70)
                if ((emp_hire_date.year >= 2009) and (emp_hire_date).year <= 2015):
                    age = random.randint(23, 65)
                if (emp_hire_date.year > 2015):
                    age = random.randint(23, 70)
                if (status_num == 1):
                    status = "active"
                else:
                    status = "inactive"
                t = time.time()
                current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))
                emp_hire_date = emp_hire_date.strftime('%Y-%m-%d')
                cmd = "INSERT INTO Cisco.Employee(status,gender,firstname,lastname,age,emp_hire_date,time_inserted) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                val = (status, employee_gender, employee_fname, employee_ln, age, emp_hire_date, current_time)
                cursor.execute(cmd, val)
                if (batchsize % int(numbatch) == 0):
                    mydb.commit()
                    print("total row commited {0}".format(batchsize))
            print("finish data insert into mysql")


def signal_handler(sig, frame):
    print("Program interrupted. Exiting...")
    timeout_handler()


def timeout_handler():
    print("Program timed out. Stopping threads...")
    for t in threads:
        t.stop();


def generate_data_thread_task(thread_num, rootuser, ipaddress, numbatch):
    #lock.acquire()
    generate_employee_data(thread_num, rootuser, ipaddress, numbatch)
    #lock.release()


def main_task_generate_data(thread_count, rootuser, ipaddress, numbatch):
    print "Running generation data tool...\nrootuser : {}\nipaddress : {}".format(rootuser,ipaddress)
    signal.signal(signal.SIGINT, signal_handler)

    start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    timeout = 3600

    print "insert mysql starttime"+str(start_time)

    # creating a lock
    #lock = threading.Lock()

    for tid in range(thread_count):
        thread = threading.Thread(target=generate_data_thread_task,args=(tid, rootuser, ipaddress, numbatch,))
        threads.append(thread)
        thread.start()
        if thread.is_alive():
            print(str(tid) + ' Thread Still running')
        else:
            print(str(tid) +' Thread Completed')

    if timeout > -1:
        timer = threading.Timer(timeout, timeout_handler)
        timer.start()

    # wait for threads to complete
    for tid in range(thread_count):
        threads[tid].join()
        print("Threading {0}:".format(tid))

    end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print "insert mysql endtime" + str(end_time)
    timer.cancel()


if __name__ == "__main__":
    print len(sys.argv)
    if len(sys.argv) != 4:
        print "Usage: python profile-generation.py <<UserID>> <<IPAddress>> <<No. Of numbatch>>"
        sys.exit()
    main_task_generate_data(10,sys.argv[1],sys.argv[2],sys.argv[3])
