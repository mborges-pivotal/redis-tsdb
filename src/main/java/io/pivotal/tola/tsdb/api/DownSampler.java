package io.pivotal.tola.tsdb.api;

import java.time.Instant;

import lombok.Data;

@Data
public class DownSampler {

	public static final long SECOND = 1000;
	public static final long MINUTE = 60 * 1000;
	public static final long HOUR = 60 * 60 * 1000;
	public static final long DAY = 24 * 60 * 60 * 1000;

	private Event startEvent;

	private long counter;
	private double total;

	private long current_ds;

	// constructor
	public DownSampler(Event e, long current_ds) {
		startEvent = e;
		this.current_ds = current_ds;
		counter = 1;
		total = e.getValue();
	}

	public static long getDownSampler(char c) {
		
		switch (c) {
		case 's':
			return SECOND;
		case 'm':
			return MINUTE;
		case 'h':
			return HOUR;
		case 'd':
			return DAY;
		default:
			return SECOND;
		}

	}

	public DownSampler add(double d) {
		counter += 1;
		total += d;
		return this;
	}

	public Event getDownSamplerEvent(long millisToAdd) {
		startEvent.setTimestamp(Instant.EPOCH.plusMillis(millisToAdd));
		startEvent.setValue(average());
		return startEvent;
	}

	private double average() {
		return total / counter;
	}

}
