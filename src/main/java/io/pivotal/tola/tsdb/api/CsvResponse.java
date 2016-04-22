package io.pivotal.tola.tsdb.api;

import java.util.Set;

/**
 * CvsResponse - Used by the HttpMessageConverter
 * 
 * @author mborges
 *
 */
public class CsvResponse {

	private final String filename;
	private final Set<Event> records;

	public CsvResponse(Set<Event> records, String filename) {
	       this.records = records;
	       this.filename = filename;
	   }

	public String getFilename() {
		return filename;
	}

	public Set<Event> getRecords() {
		return records;
	}

}
