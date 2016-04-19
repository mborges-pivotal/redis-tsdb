package io.pivotal.tola.tsdb;

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
import org.springframework.data.redis.support.atomic.RedisAtomicLong;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.google.common.base.Splitter;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;

@Service
public class TsdbService {
	
	private Log log = LogFactory.getLog(TsdbService.class);

	@Autowired
	private RedisTemplate<String, Event> events;

	@Autowired
	private StringRedisTemplate eventsIndex;

	/**
	 * get eventID using redis increment function create a pipeline: add
	 * eventId, Event and sorted set with eventId and timestamp as score
	 * 
	 * @param event
	 */
	public void recordEvent(String metric, Event event) {
		RedisAtomicLong counter = new RedisAtomicLong(eventIdCounterKey(metric), events.getConnectionFactory());
		long id = counter.incrementAndGet();
		event.setId(id);

		events.executePipelined(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {

				events.opsForHash().put(eventsKey(metric), eventIdKey(metric, id + ""), event);
				eventsIndex.opsForZSet().add(eventIndexKey(metric), event.getId() + "", event.getTimestampInMillis());

				// Adding index for each tag in the event
				for (Map.Entry<String, String> tag : event.getTags().entrySet()) {
					String key = tag.getKey();
					String value = tag.getValue();
					eventsIndex.opsForSet().add(eventTagIndexKey(metric, key, value), event.getId() + "");
				}

				return null;
			}
		});
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

	private static String eventIdKey(String metric, String id) {
		return String.format("event:%s:%s", metric, id);
	}

	private static String eventIndexKey(String metric) {
		return String.format("event:%s:index", metric);
	}

	private static String eventTagIndexKey(String metric, String tagKey, String tagValue) {
		return String.format("event:%s:%s:%s:index", metric, tagKey, tagValue);
	}
	
	// parse tags string into index keys
	// in = tags -> region=georgia,turkey well=a3
	private static Set<String> tags2keys(String metric, String in) {
		Map<String,String> tags = Splitter.on(" ").withKeyValueSeparator("=").split(in.toLowerCase());
		Set<String> setKeys = new HashSet<String>();
		for(Map.Entry<String, String> tagEntry: tags.entrySet()) {
			String tag = tagEntry.getKey();
			String[] values = tagEntry.getValue().split(",");
			for(String tagValue: values) {
				String setKey = eventTagIndexKey(metric, tag, tagValue);
				setKeys.add(setKey);
			}
		}
		return setKeys;
	}
	
	// V2
	private static MultiValueMap<String, String> tags2keys2(String metric, String in) {
		Map<String,String> tags = Splitter.on(" ").withKeyValueSeparator("=").split(in.toLowerCase());
		
		MultiValueMap<String, String> result = new LinkedMultiValueMap<String, String>();
		for(Map.Entry<String, String> tagEntry: tags.entrySet()) {
			String tag = tagEntry.getKey();
			String[] values = tagEntry.getValue().split(",");
			for(String tagValue: values) {
				String setKey = eventTagIndexKey(metric, tag, tagValue);
				result.add(tag, setKey);
			}
		}
		
		return result;
	}
	

	//////////////////
	// Basic Operations
	//////////////////

	public Event retrieveEvent(String metric, String id) {
		return (Event) events.opsForHash().get(eventsKey(metric), eventIdKey(metric, id));
	}

	@SuppressWarnings("unchecked")
	public List<Event> retrieveEvents(String metric, Set<String> ids) {
		
		Collection<Object> eventKeys = new HashSet<Object>();
		for(String id: ids) {
			eventKeys.add(eventIdKey(metric, id));
		}
		
		return (List<Event>)(List<?>)events.opsForHash().multiGet(eventsKey(metric), eventKeys);
	}
	
	public Set<String> getMetrics() {
		Set<String> metricsKeys = events.keys("events:*");
		Set<String> metrics = new HashSet<String>();
		for (String metric : metricsKeys) {
			String[] parts = metric.split(":");
			metrics.add(parts[1]);
		}
		return metrics;
	}

	public MultiValueMap<String, String> getMetricTags(String metric) {
		Set<String> keys = events.keys(String.format("event:%s:*:*:index", metric));
		MultiValueMap<String, String> tags = new LinkedMultiValueMap<String, String>();
		for (String key : keys) {
			String[] parts = key.split(":");
			tags.add(parts[2], parts[3]);
		}
		return tags;
	}

	//////////////////
	// Analytics
	//////////////////

	public long countEvents(String metric, Instant min, Instant max) {
		return events.opsForZSet().count(eventIndexKey(metric), min.toEpochMilli(), max.toEpochMilli());
	}

	public Set<String> getEventKeys(String metric, Instant min, Instant max) {
		return eventsIndex.opsForZSet().rangeByScore(eventIndexKey(metric), min.toEpochMilli(), max.toEpochMilli());
	}

	// All Keys
	public Set<String> getEventKeys(String metric, String tags) {
		return eventsIndex.opsForZSet().range(eventIndexKey(metric), 0, -1);
	}
	
	// Union
	// in = tags -> region=georgia,turkey well=a3
	public Set<String> getEventKeys(String metric, String tags, Instant min, Instant max) {

		String tempSet = "TEMP_"+(metric+tags+min+max).hashCode();
		Set<String> setKeys = tags2keys(metric, tags);
		
		// Get time range
		Set<String> r = getEventKeys(metric, min, max);
		if (r.isEmpty()) {
			log.info("############## - EMPTY RANGE");
			return r;
		}
		
		eventsIndex.opsForSet().add(tempSet+"_PRE", r.toArray(new String[r.size()]));
		
		// intersect with tags keys
		eventsIndex.opsForSet().intersectAndStore(tempSet+"_PRE", setKeys, tempSet);		
		log.info("## UNION ## " + eventsIndex.opsForSet().size(tempSet));
		Set<String> finalResult = eventsIndex.opsForSet().members(tempSet);
		
		eventsIndex.delete(tempSet+"_PRE");
		eventsIndex.delete(tempSet);
		
		return finalResult;
	}

	// Union - V2
	// in = tags -> region=georgia,turkey well=a3
	public Set<String> getEventKeys2(String metric, String tags, Instant min, Instant max) {

		if (tags == null || tags.trim().length() <=0) {
			log.info("no tags sent");
			return new HashSet<String>();
		}
		
		String tempSet = "TEMP_"+(metric+tags+min+max).hashCode();
		MultiValueMap<String, String>  setKeys = tags2keys2(metric, tags);
		
		// Get time range
		Set<String> r = getEventKeys(metric, min, max);
		if (r.isEmpty()) {
			log.info("############## - EMPTY RANGE");
			return r;
		}

		eventsIndex.opsForSet().add(tempSet+"_PRE", r.toArray(new String[r.size()]));
		
		// union of all values per tag
		Set<String> tagSets = new HashSet<String>();
		for(Map.Entry<String, List<String>> entry: setKeys.entrySet()) {
			String key = entry.getKey();
			String tempTagSet = tempSet + "_" + key;
			tagSets.add(tempTagSet);
			List<String> tagValues = entry.getValue();
			eventsIndex.opsForSet().unionAndStore("dummy", tagValues ,tempTagSet);						
		}
		
		// intersect with tags keys
		eventsIndex.opsForSet().intersectAndStore(tempSet+"_PRE", tagSets, tempSet);		
		log.info("## UNION ## " + eventsIndex.opsForSet().size(tempSet));
		Set<String> finalResult = eventsIndex.opsForSet().members(tempSet);
		
		eventsIndex.delete(tempSet+"_PRE");
		eventsIndex.delete(tempSet);
		eventsIndex.delete(tagSets);
		
		return finalResult;
	}

	//////////////////
	// LUA SCRIPTS FOR AGGREGATION
	//////////////////
	
	public double sumEvents(String metric, Instant min, Instant max) {
		Set<String> eventKeys  = getEventKeys(metric, min, max);
		double total = 0;
		
		for(String k: eventKeys) {
			Event e = retrieveEvent(metric, k);
			total += e.getValue();
		}
		return total;
	}
	
	public double avgEvents(String metric, Instant min, Instant max) {
		Set<String> eventKeys  = getEventKeys(metric, min, max);
		double total = 0;
		
		for(String k: eventKeys) {
			Event e = retrieveEvent(metric, k);
			total += e.getValue();
		}
		return total / eventKeys.size();
	}
	

}
