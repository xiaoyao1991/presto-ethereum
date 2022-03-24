JAVA_HOME=/usr/lib/jvm/java-8-openjdk mvn clean package
rm -rf /opt/presto-server-0.271/plugin/ethereum/*
tar xfz target/presto-ethereum-1.0-SNAPSHOT-plugin.tar.gz -C /opt/presto-server-0.271/plugin/ethereum --strip-components=1 
