Kafka:
- see startup script `kafka-startup.sh`. It installs the producer jar and csv files  in /opt.

Dataflow:
- grab popular-areas-beam-bundled-0.1.jar from the bucket.
    Run it with: java -cp popular-areas-beam-bundled-0.1.jar xyz.akumano.beam.popularareas.Main
- the staging location (set to gs://bucket-name/staging) is where dependencies are sent to.
    Also need to set gcpTempLocation.
- create a service account, get a json, and export it in GOOGLE\_APPLICATION\_CREDENTIALS as an environment variable (do so in .bashrc).

Popularity prediction streams:
- create a Dockerfile within the popular-areas-predictor project. Use a gcloud command to upload the build to the personal registry.
- Use the internal IP of the Kafka instance to connect to it.

Elsaticsearch:
- see `es-startup.sh`.
- beam.ElasticsearchIO only supports ES 5.x and 2.x.
- containers run on the host network mode, so supposedly port publishing flags are unnecessary.
    In reality, -p flags were necessary. Rerun it with `docker run -p 9200:9200 -p 9300:9300 CONTAINER`.
- Use `-e "xpack.security.enabled=false"` when running `docker run`.
- to run Elasticsearch, `vm.max_map_count` must be set to at least 262144 (should be higher?); the default 65530 is too low. `echo 262144 > /proc/sys/vm/max_map_count` or `sysctl -w vm.max_map_count=262144`.
- create index: `curl -XPUT "http://$ELASTICSEARCH_IP:9200/nyc-taxi"`
    create mapping:
      curl -H 'Content-Type: application/json' -XPUT "http://$ELASTICSEARCH_IP:9200/nyc-taxi/_mapping/popular-places" -d'
        {
         "popular-places" : {
           "properties" : {
              "cnt": {"type": "integer"},
              "location": {"type": "geo\_point"},
              "time": {"type": "date"}
            }
         }
        }'
- `curl elasticsearch:9200/nyc-taxi/_stats/docs` to see if records were sent ("count" should be positive)
- To post to the ES instance from a remote machine, ensure traffic on tcp:9200 is allowed. `gcloud compute firewall-rules create es-allow-http --allow=tcp:9200`

Kibana:
- see `es-startup.sh`. Kibana runs on the same instance as ES.
- `docker run -p 5601:5601 -e"ELASTICSEARCH_URL=http://blahblah:9200" -e"SERVER_HOST=0.0.0.0" CONTAINER`
- To access the service via a browser, ensure traffic on tcp:5601 is allowed. `gcloud compute firewall-rules create kibana-allow-http --allow=tcp:5601`
- Create a "Coordinate Map" in Visualization.
