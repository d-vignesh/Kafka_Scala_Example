An example implementation of kafka data pipeline in scala.

The application consists of a DataProducer class that publishes message to a kafka cluster and a DataConsumer class which consumes the data from kafka cluster.

To run the application we need a running kafka-cluster and sbt installed in our system.
  *  execute the DataConsumer class is a terminal via sbt using the command : runMain DataConsumer localhost:9092 test quickstart
  *  executed the DataProducer class in another terminal via sbt using the command : runMain DataProducer 3 quickstart localhost:9092
 
