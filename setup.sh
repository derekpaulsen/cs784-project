#install all the stuff for each node 
sudo apt update -y
sudo apt upgrade -y

#install java
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:webupd8team/java
sudo apt update
sudo apt install java-common -y
sudo apt install oracle-java8-installer

sudo apt install scala -y
sudo apt install openssh-server openssh-client -y 
#utilities
sudo apt install vim tmux htop ipython3 python3-pip -y

pip3 install pandas numpy scipy


