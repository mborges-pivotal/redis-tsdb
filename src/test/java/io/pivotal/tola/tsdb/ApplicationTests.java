package io.pivotal.tola.tsdb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import io.pivotal.tola.tsdb.api.TsdbService;
import io.pivotal.tola.tsdb.tools.SampleGenerator;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
public class ApplicationTests {

	private Log log = LogFactory.getLog(ApplicationTests.class);

	@Autowired
	private TsdbService tsdb;

	@Autowired
	private SampleGenerator generator;

	// max time used for all tests
	private ZonedDateTime zdt = ZonedDateTime.now();

	@Before
	public void generateData() {
		generator.oilGasData();
	}

	//////////////////////////////////////
	// TESTS
	////////////////////////////////////

	@Test
	public void listMetrics() throws Exception {
		assertThat(tsdb.getMetrics()).contains("temperature", "pressure");
	}

	@Test
	public void listMetricTags() throws Exception {
		assertThat(tsdb.getMetricTags("temperature")).containsKeys("well", "region");
		assertThat(tsdb.getMetricTags("temperature")).containsEntry("region",string2List("azerbaijan,georgia,turkey"));
		assertThat(tsdb.getMetricTags("temperature")).containsEntry("well",string2List("e6,c2,a4,a3,b4"));
	}

	@Test
	public void eventsInPast5Minutes() throws Exception {

		String metric = "temperature";

		Set<String> eventKeys = tsdb.getEventKeys(metric, zdt.minusMinutes(5).toInstant(), zdt.toInstant());
		System.out.printf("Metric: %s has %d events\n", metric, eventKeys.size());
		for (String k : eventKeys) {
			System.out.print(k + ":");
			System.out.println(tsdb.retrieveEvent(metric, k));
		}

	}

	@Test
	public void eventsInPast5MinutesWithTags() throws Exception {
		String metric = "temperature";

		// QUERIES
		Set<String> result = tsdb.getEventKeys(metric, "region=georgia", zdt.minusMinutes(5).toInstant(),
				zdt.toInstant());
		log.info(result);
		log.info(result.size());
		log.info(tsdb.retrieveEvents(metric, result));

		// QUERIES
		result = tsdb.getEventKeys(metric, "region=georgia", zdt.minusMinutes(5).toInstant(), zdt.toInstant());
		log.info(result);
		log.info(result.size());
		log.info(tsdb.retrieveEvents(metric, result));

	}

	@Test
	public void eventsInPast5MinutesWithMultiValueTags() throws Exception {

		String metric = "temperature";

		// QUERIES
		Set<String> result = tsdb.getEventKeys(metric, "region=georgia,turkey well=a3,a4,b4",
				zdt.minusMinutes(5).toInstant(), zdt.toInstant());
		log.info(result);
		log.info(result.size());
		log.info(tsdb.retrieveEvents(metric, result));

	}
	
	//////////////////////////////////////
	// Helper methods
	////////////////////////////////////
	private List<String> string2List(String str) {
		return new ArrayList<String>(Arrays.asList(str.split(",")));
	}

	

}
