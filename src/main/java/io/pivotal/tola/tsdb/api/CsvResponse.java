package io.pivotal.tola.tsdb.api;

import java.util.List;

/**
 * CvsResponse - Used by the HttpMessageConverter
 * 
 * @author mborges
 *
 */
public class CsvResponse {

	private final String filename;
	private final List<Event> records;

	public CsvResponse(List<Event> records, String filename) {
	       this.records = records;
	       this.filename = filename;
	   }

	public String getFilename() {
		return filename;
	}

	public List<Event> getRecords() {
		return records;
	}

}
