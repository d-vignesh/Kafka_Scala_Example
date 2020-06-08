import java.util.{Collections, Properties}
import scala.collection.JavaConversions._
import java.time.Duration
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecords, ConsumerRecord}

class DataConsumer(val borkers: String,
				   val groupId: String,
				   val topic: String) {

	var consumer: KafkaConsumer[String, String] = null

	def createConsumer(): Unit = {
		val props = createConsumerConfig(borkers, groupId)
		consumer = new KafkaConsumer[String, String](props)
		consumer.subscribe(List(topic))
	}

	def consumeData(): Unit = {
		while(true) {
			val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(1000))
			for(record: ConsumerRecord[String, String] <- records) {
				println(record.key() + " " + record.value())
			}
		}
	}

	def shutDown() = {
		if (consumer != null)
			consumer.close();
	}

	def createConsumerConfig(borkers: String, groupId: String): Properties = {
		val props = new Properties()
		props.setProperty("bootstrap.servers", "localhost:9092")
		props.setProperty("group.id", groupId)
		props.setProperty("enable.auto.commit", "true")
		props.setProperty("auto.commit.interval.ms", "1000")
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		return props 
	}
}

object DataConsumer extends App {
	val dataConsumer = new DataConsumer(args(0), args(1), args(2))
	dataConsumer.createConsumer()
	dataConsumer.consumeData()
	dataConsumer.shutDown()
}

