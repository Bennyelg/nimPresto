
FROM ubuntu:16.04
RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get -y install git
RUN apt-get -y install python
RUN apt-get -y install openjdk-8-jdk
RUN export JAVA_HOME=/usr/lib/jvm/java-8-openjdk
RUN export PATH=$PATH:$HOME/bin:$JAVA_HOME/bin
RUN apt-get -y install mysql-server mysql-client
RUN service mysql restart
RUN mysql -uroot -e "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('12345')"
ENV MYSQL='mysql AMORE -u root -p12345 -h localhost -e'
ENV QUERY="CREATE DATABASE test;
use test;
CREATE TABLE testTable(
    name varchar(64),
    lastname varchar(64),
    phoneNumber varchar(64)
);
INSERT INTO testTable(name, lastname, phoneNumber) VALUES('Benny', 'Elgazar', '012345678');
INSERT INTO testTable(name, lastname, phoneNumber) VALUES('Moshe', 'Moshe', '876543210');
INSERT INTO testTable(name, lastname, phoneNumber) VALUES('David', 'Alfonso', '987898776');
INSERT INTO testTable(name, lastname, phoneNumber) VALUES('Roman', 'Jorgionik', '678929372');
INSERT INTO testTable(name, lastname, phoneNumber) VALUES('Shoki', 'Zlodnik', '0980102943');"
RUN eval $MYSQL "'$QUERY'"
RUN apt-get -y install vim
RUN apt-get -y install wget
RUN apt-get -y install build-essential
RUN git clone https://github.com/nim-lang/Nim.git
RUN cd Nim
RUN git clone --depth 1 https://github.com/nim-lang/csources.git
RUN sh build.sh
RUN ./koch nimble
RUN export PATH=$PATH:/Nim/bin

RUN /opt
RUN mkdir presto
RUN cd presto
RUN wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.184/presto-server-0.184.tar.gz
RUN tar -zxvf presto-server-0.184.tar.gz -C /opt/presto --strip-components=1
RUN mkdir -p /opt/presto/etc/catalog

RUN echo "coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=6969
query.max-memory=500MB
discovery-server.enabled=true
discovery.uri=http://localhost:6969" > /opt/presto/etc/config.properties

RUN echo "-server
-Xmx16G
-XX:+UseConcMarkSweepGC
-XX:+ExplicitGCInvokesConcurrent
-XX:+AggressiveOpts
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p" > /opt/presto/etc/jvm.config

RUN echo "com.facebook.presto=INFO" > /opt/presto/etc/log.properties

RUN echo "node.environment=production
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
node.data-dir=/var/presto/data" > /opt/presto/etc/node.properties

RUN echo "connector.name=mysql
connection-url=jdbc:mysql://localhost:3306
connection-user=root
connection-password=12345" > /opt/presto/etc/catalog/mysql.catalog

EXPOSE 6969
EXPOSE 3306

CMD ['/opt/presto/bin/launcher.py',  'start']






