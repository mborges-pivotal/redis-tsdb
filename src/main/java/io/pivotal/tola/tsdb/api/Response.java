package io.pivotal.tola.tsdb.api;

import lombok.Data;

/**
 * Response - used to wrap the REST API responses for proper JSON serialization
 * 
 * @author mborges
 *
 */
@Data
public class Response {
	
	private Object data;
	
	public Response() {
		
	}
	
	public Response(Object data) {
		this.data = data;
	}
	
	public static Response instance(Object data) {
		return new Response(data);
	}

}
