ant
rm -rf /home/cis455/apache-tomcat-7.0.59/webapps/master /home/cis455/apache-tomcat-7.0.59/webapps/master.war
cp master.war /home/cis455/apache-tomcat-7.0.59/webapps
sh /home/cis455/apache-tomcat-7.0.59/bin/catalina.sh run
