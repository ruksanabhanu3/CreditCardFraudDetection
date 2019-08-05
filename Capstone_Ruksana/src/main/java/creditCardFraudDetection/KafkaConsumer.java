package creditCardFraudDetection;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
public class KafkaConsumer {
	
	/* Main class for this project. Kafka consumer subscribes to the topic of card transactions.
	 * It is then validated on basis of 3 rules. ucl, memberscore and zipcode distance.
	 * If the validation passes, the incoming transaction is stored in no sql db and if the transaction is gennuine,
	 * the look-up table is updated accordingly.*/
	
	public static void main(String[] args) throws InterruptedException {
		
		//setting the log levels
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkStreamingCapstoneProject").setMaster("local");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", "100.24.223.181:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", true);
		if (kafkaParams.get("group.id") == null) {
			kafkaParams.put("group.id", "upgraduserkafkaspark-" + new Random().nextInt(100000));
		}
		Collection<String> topics = Arrays.asList("transactions-topic-verified");
		
		//DStream of records from the kafka topic.
		JavaInputDStream<ConsumerRecord<String, String>> incomingStream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		JavaDStream<String> incomingDStream = incomingStream.map(x -> x.value());
		
		//filtering the contents of the header.
		JavaDStream<String> incomingDStream_filter = incomingDStream.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			public Boolean call(String row) throws Exception {
				return row.startsWith("{\"card_id\":");
			}
		});
		
		//Object CreditTransactionsUtil has the constructor which parses the incoming data and assign the value to the attributes present in that object.
		JavaDStream<CreditTransactionsUtil> final_dstream = incomingDStream_filter.map(x -> new CreditTransactionsUtil(x));
		
		
		//Calling a function for each of the incoming transactions to validate the 3 rules.
		final_dstream.foreachRDD(new VoidFunction<JavaRDD<CreditTransactionsUtil>>() {
			
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<CreditTransactionsUtil> rdd)  {
				rdd.foreach(x -> x.validateTransactionsRules(x));
			}

		});
		
		JavaDStream<Long> final_stream_count = final_dstream.count();
		final_stream_count.print();
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		jssc.close();
	}	
}
