package io.pivotal.tola.tsdb.api;

import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.support.atomic.RedisAtomicLong;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.google.common.base.Splitter;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;

/**
 * TsdbService - Implements the TSDB in Redis
 * 
 * This is about managing time series data in Redis. A data point is represented
 * in an Event object and redis data structures are used to store the events,
 * manage its unique Ids in a metric. Metric's events can be categorized by
 * using tags.
 *
 * Sample of data points – temperature (metric)
 * 
 * 30 | 2016-04-19T16:39:53.972Z | {region=georgia, well=e6} | 61.2
 * 50 | 2016-04-19T16:45:53.972Z | {region=georgia, well=a2} | 41.2
 *
 * @author mborges
 *
 */
@Service
public class TsdbService {

	private Log log = LogFactory.getLog(TsdbService.class);

	@Autowired
	private RedisTemplate<String, Event> events;

	@Autowired
	private StringRedisTemplate eventsIndex;

	/**
	 * recordEvent - Event is an object that represents a data point
	 * 
	 * get eventID using redis increment function create a pipeline: add
	 * eventId, Event and sorted set with eventId and timestamp as score
	 * 
	 * @param metric
	 * @param event
	 */
	public void recordEvent(String metric, Event event) {
		RedisAtomicLong counter = new RedisAtomicLong(eventIdCounterKey(metric), events.getConnectionFactory());
		long id = counter.incrementAndGet();
		event.setId(id);

		events.executePipelined(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {

				events.opsForHash().put(eventsKey(metric), id+"", event);
				eventsIndex.opsForZSet().add(eventIndexKey(metric), event.getId() + "", event.getTimestampInMillis());

				// Adding index for each tag in the event
				for (Map.Entry<String, String> tag : event.getTags().entrySet()) {
					String key = tag.getKey();
					String value = tag.getValue();
					eventsIndex.opsForZSet().add(eventTagIndexKey(metric, key, value), event.getId() + "", event.getTimestampInMillis());
				}

				return null;
			}
		});
	}

	/**
	 * retrieveEvent - retrieve event for a metric based on event Id
	 * 
	 * @param metric
	 * @param id
	 *            - event Id
	 * 
	 * @return event
	 */
	public Event retrieveEvent(String metric, String id) {
		return (Event) events.opsForHash().get(eventsKey(metric), id);
	}

	@SuppressWarnings("unchecked")
	public List<Event> retrieveEvents(String metric, Set<String> ids) {
		return (List<Event>) (List<?>) events.opsForHash().multiGet(eventsKey(metric), (Collection<Object>)(Collection<?>)ids);
	}

	/**
	 * getMetrics
	 * 
	 * @return a set of all available metrics
	 */
	public Set<String> getMetrics() {
		Set<String> metricsKeys = events.keys("events:*");
		Set<String> metrics = new HashSet<String>();
		for (String metric : metricsKeys) {
			String[] parts = metric.split(":");
			metrics.add(parts[1]);
		}
		return metrics;
	}

	/**
	 * getMetricTags
	 * 
	 * @param metric
	 * 
	 * @return a multiValueMap with all tags for a particular metric
	 */
	public MultiValueMap<String, String> getMetricTags(String metric) {
		Set<String> keys = events.keys(String.format("event:%s:*:*:index", metric));
		MultiValueMap<String, String> tags = new LinkedMultiValueMap<String, String>();
		for (String key : keys) {
			String[] parts = key.split(":");
			tags.add(parts[2], parts[3]);
		}
		return tags;
	}

	/**
	 * countEvents
	 * 
	 * @param metric
	 * @param min
	 *            - beginning of the time window
	 * @param max
	 *            - end of the time window
	 * 
	 * @return number of events for a particular time window
	 */
	public long countEvents(String metric, Instant min, Instant max) {
		return events.opsForZSet().count(eventIndexKey(metric), min.toEpochMilli(), max.toEpochMilli());
	}

	/**
	 * getEventKeys
	 * 
	 * @param metric
	 * @param min
	 *            - beginning of the time window
	 * @param max
	 *            - end of the time window
	 * 
	 * @return a set of all keys in a time window for a particular metric
	 */
	public Set<String> getEventKeys(String metric, Instant min, Instant max) {
		return eventsIndex.opsForZSet().rangeByScore(eventIndexKey(metric), min.toEpochMilli(), max.toEpochMilli());
	}
	
	/**
	 * getEventKeys - ATTENTION, this can return a lot of data points
	 * 
	 * @param metric
	 * @param tags
	 * 
	 * @return all keys for a metric filter by tags
	 */
	public Set<String> getEventKeys(String metric, String tags) {
		return eventsIndex.opsForZSet().range(eventIndexKey(metric), 0, -1);
	}

	/**
	 * getEventKeys
	 * 
	 * You can provide a list of values for a particular tag.
	 * 
	 * @param metric
	 * @param tags
	 *            - String representing tag and its values.
	 *            eg.region=georgia,turkey well=a3
	 * @param min
	 *            - beginning of the time window
	 * @param max
	 *            - end of the time window
	 * 
	 * @return
	 */
	public Set<String> getEventKeys(String metric, String tags, Instant min, Instant max) {

		String tempSet = "TEMP_" + (metric + tags + min + max).hashCode();
		MultiValueMap<String, String> setKeys = tags2keys(metric, tags);

		// Get time range and create 
		Set<ZSetOperations.TypedTuple<String>> r = getEventKeysWithScores(metric, min, max);
		if (r.isEmpty()) {
			log.info("############## - EMPTY RANGE");
			return new HashSet<String>();
		}
		eventsIndex.opsForZSet().add(tempSet + "_PRE", r);

		// union of all values per tag
		Set<String> tagSets = new HashSet<String>();
		for (Map.Entry<String, List<String>> entry : setKeys.entrySet()) {
			String key = entry.getKey();
			String tempTagSet = tempSet + "_" + key;
			tagSets.add(tempTagSet);
			List<String> tagValues = entry.getValue();
			eventsIndex.opsForZSet().unionAndStore("dummy", tagValues, tempTagSet);
		}

		// intersect with tags keys
		eventsIndex.opsForZSet().intersectAndStore(tempSet + "_PRE", tagSets, tempSet);
		log.info("## UNION ## " + eventsIndex.opsForZSet().size(tempSet));
		Set<String> finalResult = eventsIndex.opsForZSet().range(tempSet, 0, -1);

		eventsIndex.delete(tempSet + "_PRE");
		eventsIndex.delete(tempSet);
		eventsIndex.delete(tagSets);

		return finalResult;
	}

	//////////////////
	// Key Management
	//////////////////

	private static String eventsKey(String metric) {
		return String.format("events:%s", metric);
	}

	private static String eventIdCounterKey(String metric) {
		return String.format("event:%s:counter", metric);
	}

	/*
	private static String eventIdKey(String metric, String id) {
		return String.format("event:%s:%s", metric, id);
	}
	*/

	private static String eventIndexKey(String metric) {
		return String.format("event:%s:index", metric);
	}

	private static String eventTagIndexKey(String metric, String tagKey, String tagValue) {
		return String.format("event:%s:%s:%s:index", metric, tagKey, tagValue);
	}

	/**
	 * tags2keys - parse tag String
	 * 
	 * @param metric
	 * @param in - tag string. e.g. region=georgia,turkey well=a3
	 * 
	 * @return tag keys and it's values
	 */
	private static MultiValueMap<String, String> tags2keys(String metric, String in) {
		Map<String, String> tags = Splitter.on(" ").withKeyValueSeparator("=").split(in.toLowerCase());

		MultiValueMap<String, String> result = new LinkedMultiValueMap<String, String>();
		for (Map.Entry<String, String> tagEntry : tags.entrySet()) {
			String tag = tagEntry.getKey();
			String[] values = tagEntry.getValue().split(",");
			for (String tagValue : values) {
				String setKey = String.format("event:%s:%s:%s:index", metric, tag, tagValue);
				result.add(tag, setKey);
			}
		}

		return result;
	}

	//////////////////
	// LUA SCRIPTS FOR AGGREGATION
	//////////////////

	private Set<ZSetOperations.TypedTuple<String>> getEventKeysWithScores(String metric, Instant min, Instant max) {
		return eventsIndex.opsForZSet().rangeByScoreWithScores(eventIndexKey(metric), min.toEpochMilli(), max.toEpochMilli());
	}	
	
	public double sumEvents(String metric, Instant min, Instant max) {
		Set<String> eventKeys = getEventKeys(metric, min, max);
		double total = 0;

		for (String k : eventKeys) {
			Event e = retrieveEvent(metric, k);
			total += e.getValue();
		}
		return total;
	}

	public double avgEvents(String metric, Instant min, Instant max) {
		Set<String> eventKeys = getEventKeys(metric, min, max);
		double total = 0;

		for (String k : eventKeys) {
			Event e = retrieveEvent(metric, k);
			total += e.getValue();
		}
		return total / eventKeys.size();
	}

}
