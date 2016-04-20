package io.pivotal.tola.tsdb.tools;

import java.time.ZonedDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.pivotal.tola.tsdb.api.Event;
import io.pivotal.tola.tsdb.api.TsdbService;

/**
 * SampleGenerator -  Use for generating test and sample data
 * 
 * @author mborges
 *
 */
@Service
public class SampleGenerator {

	@Autowired
	private TsdbService tsdb;

	public void oilGasData() {
		String[] metrics = {"temperature", "pressure"};
		Integer[] offset = {1, 700};
		String[] regions = { "azerbaijan", "Georgia", "TuRkEy" };
		String[] wells = { "A3", "A4", "B4", "E6", "C2" };
	
		ZonedDateTime zdt = ZonedDateTime.now();
		
		// recording events
		Event e = null;
		for (int i = 0; i < 1000; i++) {
			e = new Event();
			e.setTimestamp(zdt.minusMinutes(i).toInstant());
			e.addTags(String.format("region=%s well=%s", regions[(i % regions.length)], wells[(i % wells.length)]));
			e.setValue(2.2d + i + offset[i % offset.length]);
			tsdb.recordEvent(metrics[(i % metrics.length)], e);
		}
	}

}
