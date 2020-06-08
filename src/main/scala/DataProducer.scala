import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


class DataProducer(val events: Int,
				   val topic: String,
				   val brokers: String) {

	var producer: KafkaProducer[String, String] = null

	def createProducer(): Unit = {
		val props = createProducerConfig(brokers)
		producer = new KafkaProducer[String, String](props)
	}
	
	def produceData(): Unit = {
		for(event <- Range(0, events)) {
			print("executing " + event)
			val k = Integer.toString(event)
			val v = "this is message " + k
			val data = new ProducerRecord[String, String](topic, k, v)

			producer.send(data)
		}
	}

	def shutDown(): Unit = {
		producer.flush()
		producer.close()
	}

	def createProducerConfig(brokers:String): Properties = {
		val props = new Properties()
		props.put("bootstrap.servers", brokers)
		props.put("acks", "all")
		props.put("client.id", "DataProducer")
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		props.put("auto.offset.reset", "latest")
		return props 
	}
}

object DataProducer extends App {
	val dataProducer = new DataProducer(args(0).toInt, args(1), args(2))
	dataProducer.createProducer()
	dataProducer.produceData()
	dataProducer.shutDown()
}