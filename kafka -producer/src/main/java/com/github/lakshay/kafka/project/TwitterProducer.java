package com.github.lakshay.kafka.project;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	//add your own credentials
	String consumerKey="";
	String consumerSecret="";
	String token="";
	String secret="";
	
	//add keyword of your preference
	List<String> terms = Lists.newArrayList("covid");
	
	public TwitterProducer() {}

	public static void main(String[] args)
	{
		new TwitterProducer().run();
		
	}
	public void run()
	{
		logger.info("SETUP");
		
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		
		
		// create a twitter client
		Client client = createTwitterClient(msgQueue);
		
		// Attempts to establish a connection.
		client.connect();

		// create a producer
				KafkaProducer <String, String> producer = createKafkaProducer();
		
		// loop to send tweets to Kafka.
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
		  String msg = null;
		try {
			msg = msgQueue.poll(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			client.stop();
		}
		  if (msg != null) 
		  {
			  logger.info(msg);
			  producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception e) {
					// TODO Auto-generated method stub
					if(e != null) {
						logger.error("SOMETHING BAD HAPENNED", e);
					}
				}
			    
			  });
		  }
		}
		logger.info("END OF APPLICATION");
	}
	

	public Client createTwitterClient(BlockingQueue<String> msgQueue) 
	{
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Twitter_Client")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));
					Client hosebirdClient = builder.build();
					return hosebirdClient;
	}
	
	public KafkaProducer<String, String> createKafkaProducer()
	{
		String bootstrapServers="localhost:9092";
	
		//create Producer properties
			Properties properties = new Properties();			
			properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);			
			properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());			
			properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			
		// create a safe producer
			properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
			properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
			properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
			properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
	
		// transforming our producer to be a high throughput producer (while introducing some latency (in ms) and a bit of CPU usage).
		// we'll be using Google's SNAPPY for compression, change the waiting time to 20ms, and increase the batch size from 16KB to 32KB.
			properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
			properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
			properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
			
			
		//create producer 
			KafkaProducer<String, String> producer= new KafkaProducer<String, String>(properties);
			return producer;
		
	}
}
