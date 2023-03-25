## Big Data Technologies lab 1 - Hadoop, HDFS

Бизнес логика:
Программа, которая агрегирует сырые метрики в выбранный диапазон:

Входной формат: metricId, timestamp (millis), value (справочник обозначений: metricId - metricName)

Выходной формат: metricId, timestamp, scale, value

Output Format: SequenceFile

Дополнительные требования: Использование Combiner’а.


## How to run the project:

1) `git clone https://github.com/AntonyBazin/DS_BDA_lab1`
2) `cd DS_BDA_lab1`
3) To generate the input data use **generateInputData.sh**, for example, like this:
```./generateInputData.sh logs 5 5```

    > Note: This can take a while since the script uses `sleep`.
Worst case: (num_lines (in the script, currently 50) * time_scale) seconds
4) [Optional] Add some malformed strings to **input/logs**.
5) Format **namenode**, start **HDFS** and **yarn**:
```Bash
/opt/hadoop-2.10.2/bin/hdfs namenode -format
/opt/hadoop-2.10.2/sbin/start-dfs.sh
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/your_user_name
/opt/hadoop-2.10.2/sbin/start-yarn.sh
```
6) Run the tests with Maven:
```Bash
mvn test
```
7) Create package with Maven:
```Bash
mvn package
```
8) Run the job:
```Bash
./run_cycle.sh
```
9) Search for results in `http://localhost:50070` and `http://localhost:8088/cluster`

> ### System configuration for the test machine
> Laptop, OS: [openSUSE Tumbleweed](https://www.opensuse.org/)
> 
> Kernel: Linux 6.2.1-1-default
> 
> Java: 1.8.0_362, vendor: IcedTea
> 
> Hadoop: 2.10.2
> 
> Maven: Apache Maven 3.8.6 (SUSE 3.8.6-2.2)
