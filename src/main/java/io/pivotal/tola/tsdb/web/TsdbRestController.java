package io.pivotal.tola.tsdb.web;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.pivotal.tola.tsdb.Event;
import io.pivotal.tola.tsdb.TsdbService;

@RestController
public class TsdbRestController {

	private Log log = LogFactory.getLog(TsdbRestController.class);

	@Autowired
	private TsdbService tsdb;

	@RequestMapping(value = "/tsdb/metrics", produces = "application/json")
	public Response metrics() {
		return Response.instance(tsdb.getMetrics());
	}

	@RequestMapping(value = "/tsdb/{metric}/tags")
	public Response getTags(@PathVariable String metric) {
		MultiValueMap<String, String> tags = tsdb.getMetricTags(metric);
		return Response.instance(tags);
	}

	// http://www.petrikainulainen.net/programming/spring-framework/spring-from-the-trenches-parsing-date-and-time-information-from-a-request-parameter/
	@RequestMapping(value = "/tsdb/{metric}")
	public Response getEvents(@PathVariable String metric, @RequestParam(value = "tags") String tags,
			@DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start,
			@DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime end) {
		return null;
	}

	/*
	 * // http://opentsdb.net/docs/build/html/user_guide/query/dates.html s -
	 * Seconds m - Minutes h - Hours d - Days (24 hours) w - Weeks (7 days) n -
	 * Months (30 days) y - Years (365 days)
	 * EQUAL = %3D
	 * SPACE = %20
	 * /tsdb/pressure/relative?tags=region%3dturkey&time=1h
	 * /tsdb/pressure/relative?tags=region%3dturkey,georgia%20well%3da3&time=1h
	 */
	@RequestMapping(value = "/tsdb/{metric}/relative")
	public Response getEventsRelative(@PathVariable String metric, @RequestParam(value = "tags") String tags,
			@RequestParam(value = "time", required = false) String time) {

		Response empty = Response.instance(new HashSet<String>());

		Instant max = Instant.now();
		Set<String> ids = null;

		if (time != null) {
			java.util.regex.Pattern r = java.util.regex.Pattern.compile("(\\d+)([smhdwny])");
			java.util.regex.Matcher m = r.matcher(time);
			if (m.find()) {
				log.info(m.groupCount());
				log.info(m.group(0));
				log.info(m.group(1));
				log.info(m.group(2));
			} else {
				log.info("NO MATCH");
				return empty;
			}

			char unit = m.group(2).charAt(0);
			int amount = Integer.valueOf(m.group(1));
			
			log.info(String.format("Running event query for metric %s, tags %s with time %d%s", metric, tags, amount, unit));
			
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
				return empty;
			}

			ids = tsdb.getEventKeys2(metric, tags, min, max);

		} else {
			ids = tsdb.getEventKeys(metric, tags);
		} // if time

		List<Event> events = tsdb.retrieveEvents(metric, ids);

		for (Event e : events) {
			log.info(e);
		}

		return Response.instance(events);

	}

}
