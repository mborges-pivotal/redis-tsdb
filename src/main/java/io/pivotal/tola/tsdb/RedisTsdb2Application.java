package io.pivotal.tola.tsdb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import io.pivotal.tola.tsdb.tools.SampleGenerator;

import java.time.ZonedDateTime;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;

@SpringBootApplication
public class RedisTsdb2Application implements CommandLineRunner {

	private Log log = LogFactory.getLog(RedisTsdb2Application.class);

	@Autowired
	private TsdbService tsdb;
	
	@Autowired
	private SampleGenerator generator;

	@Override
	public void run(String... args) throws Exception {
		
		generator.oilData();
		
		//////////////////////////////////////
		// TESTS
		////////////////////////////////////
		
		ZonedDateTime zdt = ZonedDateTime.now();
		String metric = "temperature";

		// getEvents
		Set<String> eventKeys = tsdb.getEventKeys(metric, zdt.minusMinutes(5).toInstant(), zdt.toInstant());
		System.out.printf("Metric: %s has %d events\n", metric, eventKeys.size());
		for (String k : eventKeys) {
			System.out.print(k + ":");
			System.out.println(tsdb.retrieveEvent(metric, k));
		}

		System.out.printf("Counter: %d\n", tsdb.countEvents(metric, zdt.minusMinutes(5).toInstant(), zdt.toInstant()));
		System.out.printf("Average: %f\n", tsdb.avgEvents(metric, zdt.minusMinutes(5).toInstant(), zdt.toInstant()));
		System.out.printf("Sum: %f\n", tsdb.sumEvents(metric, zdt.minusMinutes(5).toInstant(), zdt.toInstant()));

		System.out.println(tsdb.getMetrics());
		System.out.println(tsdb.getMetricTags(metric));
		
		// QUERIES
		Set<String> result = tsdb.getEventKeys(metric, "region=georgia", zdt.minusMinutes(5).toInstant(), zdt.toInstant());
		log.info(result);
		log.info(result.size());
		log.info(tsdb.retrieveEvents(metric, result));

		// QUERIES
		result = tsdb.getEventKeys2(metric, "region=georgia", zdt.minusMinutes(5).toInstant(), zdt.toInstant());
		log.info(result);
		log.info(result.size());
		log.info(tsdb.retrieveEvents(metric, result));
		
		// QUERIES
		result = tsdb.getEventKeys2(metric, "region=georgia,turkey well=a3,a4,b4", zdt.minusMinutes(5).toInstant(), zdt.toInstant());
		log.info(result);
		log.info(result.size());
		log.info(tsdb.retrieveEvents(metric, result));
		
	}

	public static void main(String[] args) throws Exception {
		// Close the context so it doesn't stay awake listening for redis
		// SpringApplication.run(RedisTsdb2Application.class, args).close();
		SpringApplication.run(RedisTsdb2Application.class, args);
	}

}
