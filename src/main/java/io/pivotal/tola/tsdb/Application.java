package io.pivotal.tola.tsdb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import io.pivotal.tola.tsdb.api.CsvMessageConverter;
import io.pivotal.tola.tsdb.tools.SampleGenerator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;

@SpringBootApplication
public class Application implements CommandLineRunner {

	@Autowired
	private SampleGenerator generator;

	@Override
	public void run(String... args) throws Exception {
		generator.oilGasData();
	}

	public static void main(String[] args) throws Exception {
		// Close the context so it doesn't stay awake listening for redis
		// SpringApplication.run(RedisTsdb2Application.class, args).close();
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public CsvMessageConverter csvMessageConverter() {
		return new CsvMessageConverter();
	}

}
