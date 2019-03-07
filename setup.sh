#install all the stuff for each node 
sudo apt update -y
sudo apt upgrade -y


sudo apt install java-common -y
sudo apt install scala -y
sudo apt-get install openssh-server openssh-client -y 

# spark
wget https://www.apache.org/dyn/closer.lua/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
tar xvf spark-2.4.0-bin-hadoop2.7.tgz

# hdfs 
wget http://apache.claz.org/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz
tar xvf hadoop-3.2.0.tar.gz




