from datetime import datetime
import json
import matplotlib.pyplot as plt
generateQVDdef = ""
with open(generateQVDdef, 'r') as f:
    define_json = json.load(f)
    index = 9
    collectors = {}
for item in define_json['apps']:
    if item['type'] == 'monthly':
        if index < 10:
            name = '00' + str(index)
        else:
            name = '0' + str(index)
        collectors[name] = item['loader']['names']
        index += 1
data_set = {}
for collector_list in collectors.values():
    for collector in collector_list: data_set[collector] = {'duration' : 0}
test = ""
def cleaning_line(line):
    if line[0] != '[':
        return (-1, -1, -1, -1)
    time_string = line[1:24]
    datetime_object = datetime.strptime(time_string, '%Y-%m-%dT%H:%M:%S.%f')
    node = ""
    if "rpsjqlkhsn" in line:
        node = line[line.find("rpsjqlkhsn") + 10: line.find("rpsjqlkhsn")
                                                  + 13]
    start_index = -1
    if "loading: " in line:
        start_index = int(line[line.find("loading: ") + 8: line.find("loading: ") + 11].strip())
    end_index = -1
    if "for index: " in line:
        end_index = int(line[test.find("for index: ") + 10: line.find("for index: ") + 13].strip())
    return (datetime_object, node, start_index, end_index)

f = open("./log/2020-02-14.log")
line = f.readline()
while line:
    date, node, start, end = cleaning_line(line)
    if start == -1 and end == -1:
        line = f.readline()
        continue
    if start != -1:
        collector_name = collectors[node][start]
        data_set[collector_name]['start_time'] = date
    else:
        collector_name = collectors[node][end]
        data_set[collector_name]['end_time'] = date
    line = f.readline()
f.close()

userlevel = {}
telephone = {}
PII = {}
meet = {}
for name in data_set:
    duration = data_set[name]['end_time'] - data_set[name]['start_time']
    data_set[name]['duration'] = duration.seconds
    if "" in name: userlevel[name] = duration.seconds
    if "" in name: telephone[name] = duration.seconds
    if "" in name: PII[name] = duration.seconds
    if "" in name: meet[name] = duration.seconds
userlevel = {k: v for k, v in sorted(userlevel.items(), key=lambda item: item[1], reverse = True)}
for item in userlevel.items():
    print(item)
node_running_time = {} for node in collectors:
    time = 0
    for collector in collectors[node]:
        time += data_set[collector]['duration']
        node_running_time[node] = time
    print(node_running_time)


x = node_running_time.keys()
y = [seconds / (60 * 5) for seconds in node_running_time.values()] plt.bar(x, y)
plt.title('each node running time in minutes')
plt.xlabel('node')
plt.ylabel('minutes')
plt.show()

