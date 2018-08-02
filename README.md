All Java projects use Maven.
The Spark project is built with sbt.

- `popular-areas-core/`:
    Classes and utilities to work with Kafka.

- `popular-areas-beam/`:
    Cloud Dataflow pipeline that performs a windowed aggregate of rides by pick-up locations.

- `spark-jpmml/`:
    Trains a random forest model that predicts demands per location.

- `popular-areas-predictor/`:
    Kafka streams app where the random forest model from Spark is deployed.

- `scikit/`:
    Jupyter notebooks for prototyping data pre-processing.

- `deploy/`:
    Startup scripts for GCP and deployment notes.

- `data/`:
    Ride and weather data.
