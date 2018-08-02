DOCKER_DEB_URI=https://download.docker.com/linux/debian/dists/stretch/pool/stable/amd64/docker-ce_17.03.2~ce-0~debian-stretch_amd64.deb

cd /opt
apt-get update
wget DOCKER_DEB_URI -O docker.deb
dpkg -i docker.deb
apt-get -y install -f 

sysctl -w vm.max_map_count=262144

docker run -d -p 9200:9200 -p 9300:9300 -e"xpack.security.enabled=false" docker.elastic.co/elasticsearch/elasticsearch:5.6.10
#echo "Waiting for elasticsearch to start..."
sleep 30
ELASTICSEARCH_IP=localhost
curl -XPUT "http://$ELASTICSEARCH_IP:9200/nyc-taxi"
curl -H 'Content-Type: application/json' -XPUT "http://$ELASTICSEARCH_IP:9200/nyc-taxi/_mapping/popular-places" -d'
        { 
         "popular-places" : {
           "properties" : {
              "cnt": {"type": "integer"},
              "location": {"type": "geo_point"},
              "time": {"type": "date"}
            }
         }
     }'

docker run --net=host -d -p 5601:5601 -e"ELASTICSEARCH_URL=http://$ELASTICSEARCH_IP:9200" -e"SERVER_HOST=0.0.0.0" docker.elastic.co/kibana/kibana:5.6.10
