package io.pivotal.tola.tsdb.api;

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
	
	public long getTimestampInMillis() {
		return getTimestamp().toEpochMilli();
	}
	
	public void addTag(String key, String value) {
		tags.put(key,  value);
	}
	
	// region=4 well=X"
	public void addTags(String tagList) {
		tags = splitToMap(tagList);
	}
	
	/////////////////////////////////////
	// Helper methods
	/////////////////////////////////////
	
	private Map<String, String> splitToMap(String in) {
        return Splitter.on(" ").withKeyValueSeparator("=").split(in.toLowerCase());
    }

}
