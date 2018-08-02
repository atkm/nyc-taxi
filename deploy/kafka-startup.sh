#!/usr/bin/env bash

###
# A startup script for a Compute Engine instance.
# ref: https://sookocheff.com/post/kafka/deploying-kafka-to-gce/

STARTUP_VERSION=1
STARTUP_MARK=/opt/startup.script.$STARTUP_VERSION

# Exit if this script has already ran
if [[ -f $STARTUP_MARK ]]; then
  exit 0
fi

set -o nounset
set -o pipefail
set -o errexit

SCALA_VERSION=2.11
KAFKA_VERSION=1.1.0
KAFKA_HOME=/opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"

# Install prerequesites
apt-get update
apt-get install -y wget supervisor openjdk-8-jre

# Download Kafka
wget -q http://www-us.apache.org/dist/kafka/"$KAFKA_VERSION"/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz
#wget -q http://apache.mirrors.spacedump.net/kafka/"$KAFKA_VERSION"/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz

tar xfz /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -C /opt
rm /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz

# Configure Supervisor
cat <<EOF > /etc/supervisor/conf.d/zookeeper.conf
[program:zookeeper]
command=$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
autostart=true
autorestart=true
EOF

cat <<EOF > /etc/supervisor/conf.d/kafka.conf
[program:kafka]
command=$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
autostart=true
autorestart=true
EOF

# Run
supervisorctl reread
supervisorctl update

GS_BUCKET=gs://nyc-taxi-8472
gsutil cp $GS_BUCKET/popular-areas-core-bundled-0.1.jar /opt
gsutil cp $GS_BUCKET/lga-2014-05-metar.csv /opt
gsutil cp $GS_BUCKET/lga-2014-05-01-metar.csv /opt
gsutil cp $GS_BUCKET/yellow_tripdata_2014-05-small.csv /opt
# gsutil cp $GS_BUCKET/yellow_tripdata_2014-05.csv /opt #big file

# TODO: figure out why the topics are not created. Maybe supervisorctl doesn't block, and kafka isn't started yet.
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic raw-rides
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic metar

touch $STARTUP_MARK
