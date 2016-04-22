package io.pivotal.tola.tsdb.api;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * TsdbRestController - REST API for the Redis TSDB
 * 
 * @author mborges
 *
 */
@RestController
public class TsdbRestController {

	private Log log = LogFactory.getLog(TsdbRestController.class);

	// private Response EMPTY_RESPONSE = Response.instance(new
	// HashSet<String>());

	@Autowired
	private TsdbService tsdb;

	/**
	 * metrics
	 * 
	 * @return list of metrics
	 */
	@RequestMapping(value = "/tsdb/metrics", produces = "application/json")
	public Response getMetrics() {
		return Response.instance(tsdb.getMetrics());
	}

	/**
	 * tags
	 * 
	 * @param metric
	 * 
	 * @return list of tags for a provided metric
	 */
	@RequestMapping(value = "/tsdb/{metric}/tags")
	public Response getTags(@PathVariable String metric) {
		MultiValueMap<String, String> tags = tsdb.getMetricTags(metric);
		return Response.instance(tags);
	}

	/**
	 * events - NOT READY
	 *
	 * //
	 * http://www.petrikainulainen.net/programming/spring-framework/spring-from-
	 * the-trenches-parsing-date-and-time-information-from-a-request-parameter/
	 * 
	 * @param metric
	 * @param tags
	 * @param start
	 * @param end
	 * 
	 * @return events baseed on the provided parameters
	 */
	@RequestMapping(value = "/tsdb/{metric}", method = RequestMethod.POST)
	public Response getEvents(@PathVariable String metric, @RequestParam(value = "tags") String tags,
			@DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start,
			@DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime end) {
		return null;
	}

	/**
	 * eventsRelative
	 *
	 * // http://opentsdb.net/docs/build/html/user_guide/query/dates.html s -
	 * Seconds m - Minutes h - Hours d - Days (24 hours) w - Weeks (7 days) n -
	 * Months (30 days) y - Years (365 days)
	 * 
	 * @param metric
	 * @param tags
	 * @param time
	 *            - relative time (ago). e.g 1h, 5m, 10s 1d
	 * @return
	 *
	 * 		Sample CURL command
	 * 
	 *         /tsdb/pressure/relative?tags=region%3dturkey&time=1h
	 *         /tsdb/pressure/relative?tags=region%3dturkey,georgia%20well%3da3&
	 *         time=1h
	 * 
	 *         EQUAL = %3D SPACE = %20
	 * 
	 */
	@RequestMapping(value = "/tsdb/{metric}/relative")
	public Response getEventsRelative(@PathVariable String metric, @RequestParam(value = "tags") String tags,
			@RequestParam(value = "time", required = false) String time, @RequestParam(value = "sampler", required = false) char sampler) {

		Instant max = Instant.now();
		log.info(String.format("Running event query for metric %s, tags %s with time %s", metric, tags, time));

		Instant min = relativeInstant(time, max);
		
		return Response.instance(tsdb.getEvents(metric, tags, min, max, DownSampler.getDownSampler(sampler)));

	}

	/**
	 * eventRelativeCsv download csv files for using with external applications
	 * 
	 * @param metric
	 * @param tags
	 * @param time
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "/tsdb/{metric}/relative.csv", method = RequestMethod.GET, produces = "text/csv")
	public CsvResponse getEventsRelativeCsv(@PathVariable String metric, @RequestParam(value = "tags") String tags,
			@RequestParam(value = "time", required = false) String time, @RequestParam(value = "sampler", required = false) char sampler) throws IOException {
	      @SuppressWarnings("unchecked")
	      
	      // TODO - get tag keys,values and use as part of the name
	      
		Set<Event> allRecords = (Set<Event>)getEventsRelative(metric, tags, time, sampler).getData();
	      return new CsvResponse(allRecords, metric + ".csv");
	}

	///////////////////////////////////////////
	// Helper methods
	///////////////////////////////////////////

	private Pattern r = java.util.regex.Pattern.compile("(\\d+)([smhdwny])");

	/**
	 * relativeInstant - parses time value and construct relative instance based
	 * on provided max instant
	 * 
	 * @param time
	 * @param max
	 * 
	 * @return instant or null
	 */
	private Instant relativeInstant(String time, Instant max) {

		// default for 1 day
		if (time == null) {
			time = "1d";
		}

		Matcher m = r.matcher(time);

		if (!m.find()) {
			log.info("NO MATCH for time: " + time);
			return null;
		}

		char unit = m.group(2).charAt(0);
		int amount = Integer.valueOf(m.group(1));

		Instant min = null;

		switch (unit) {
		case 's':
			min = max.minus(Duration.ofSeconds(amount));
			break;
		case 'm':
			min = max.minus(Duration.ofMinutes(amount));
			break;
		case 'h':
			min = max.minus(Duration.ofHours(amount));
			break;
		case 'd':
			min = max.minus(Duration.ofDays(amount));
			break;
		default:
			return null;
		}

		return min;
	}

}
