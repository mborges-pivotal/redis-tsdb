package io.pivotal.tola.tsdb.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Splitter;

import java.io.Serializable;
import java.time.Instant;

import lombok.Data;

@Data
public class Event implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private long id;
	
	@JsonSerialize(using = ToStringSerializer.class)
	private Instant timestamp;
	
	private Map<String,String> tags;
	private double value;
	
	// no args
	public Event() {
		
	}
	
	////////////////////////////////////////////
	
	public void resetTags() {
		tags = new HashMap<String, String>();
	}
	
	public long getTimestampInMillis() {
		return getTimestamp().toEpochMilli();
	}
	
	public long geTimestampInSeconds() {
		return getTimestamp().getEpochSecond();
	}
	
	public void addTag(String key, String value) {
		tags.put(key,  value);
	}
	
	// region=4 well=X"
	public void addTags(String tagList) {
		tags = splitToMap(tagList);
	}
	
	// Use by CSVWriter
	public String[] toStringArray() {
		List<String> l = new ArrayList<String>();
		l.add(geTimestampInSeconds()+"");
		l.add(value+"");
		l.add(tags.toString());
		return l.toArray(new String[l.size()]);
	}
	
	/////////////////////////////////////
	// Helper methods
	/////////////////////////////////////
	
	private Map<String, String> splitToMap(String in) {
        return Splitter.on(" ").withKeyValueSeparator("=").split(in.toLowerCase());
    }

}
