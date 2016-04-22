package io.pivotal.tola.tsdb.api;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Set;

import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import com.opencsv.CSVWriter;

public class CsvMessageConverter extends AbstractHttpMessageConverter<CsvResponse> {

	public static final MediaType MEDIA_TYPE = new MediaType("text", "csv", Charset.forName("utf-8"));

	public CsvMessageConverter() {
		super(MEDIA_TYPE);
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return CsvResponse.class.equals(clazz);
	}

	@Override
	protected void writeInternal(CsvResponse response, HttpOutputMessage output)
			throws IOException, HttpMessageNotWritableException {
		output.getHeaders().setContentType(MEDIA_TYPE);
		output.getHeaders().set("Content-Disposition", "attachment; filename=\"" + response.getFilename() + "\"");
		OutputStream out = output.getBody();
		CSVWriter writer = new CSVWriter(new OutputStreamWriter(out), '\t', CSVWriter.NO_QUOTE_CHARACTER);
		Set<Event> allRecords = response.getRecords();
		for (Event event: allRecords) {
			writer.writeNext(event.toStringArray());
		}
		writer.close();
	}

	@Override
	protected CsvResponse readInternal(Class<? extends CsvResponse> clazz, HttpInputMessage inputMessage)
			throws IOException, HttpMessageNotReadableException {
		return null;
	}
}
