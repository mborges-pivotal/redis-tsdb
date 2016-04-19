package io.pivotal.tola.tsdb.web;

import lombok.Data;

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
