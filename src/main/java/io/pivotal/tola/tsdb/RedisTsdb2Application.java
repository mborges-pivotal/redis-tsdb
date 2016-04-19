package io.pivotal.tola.tsdb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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

	@Override
	public void run(String... args) throws Exception {
		
		String[] metrics = {"temperature", "pressure"};
		String[] regions = { "azerbaijan", "Georgia", "TuRkEy" };
		String[] wells = { "A3", "A4", "B4", "E6", "C2" };

		ZonedDateTime zdt = ZonedDateTime.now();
		
		// recording events
		Event e = null;
		for (int i = 0; i < 1000; i++) {
			e = new Event();
			e.setTimestamp(zdt.minusMinutes(i).toInstant());
			e.addTags(String.format("region=%s well=%s", regions[(i % regions.length)], wells[(i % wells.length)]));
			e.setValue(2.2d + i);
			tsdb.recordEvent(metrics[(i % metrics.length)], e);
		}
		
		//////////////////////////////////////
		// TESTS
		////////////////////////////////////
		
		String metric = metrics[0];

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
