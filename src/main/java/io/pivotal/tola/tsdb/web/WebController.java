package io.pivotal.tola.tsdb.web;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import io.pivotal.tola.tsdb.TsdbService;

@Controller
public class WebController {
	
	private Log log = LogFactory.getLog(WebController.class);

	@Autowired
	private TsdbService tsdb;
	
	@Autowired
	private TsdbRestController api;

	/**
	 * INDEX
	 * 
	 * @param model
	 * @return
	 * @throws Exception
	 */
	@RequestMapping("/")
	public String index(Model model) throws Exception {
		Set<String> metrics = tsdb.getMetrics();
		model.addAttribute("metrics", metrics);
		model.addAttribute("tags", tsdb.getMetricTags(metrics.iterator().next()));
		model.addAttribute("events",Response.instance(new HashSet<String>()));
		return "index";
	}

	@RequestMapping(value = "/events", method = RequestMethod.POST)
	public String addAttendee(HttpServletRequest request, HttpServletResponse response, Model model) throws Exception {
		
		String tags = "";
		
		Map<String, String[]> paramMap = request.getParameterMap();
		for(Map.Entry<String, String[]> entry: paramMap.entrySet()) {
			String key = entry.getKey();
			String[] values = entry.getValue();
			if (!"metrica|amount|unit".contains(key)) {
				String tag = key + "=";
				for(int i=0; i < values.length; i++) {
					if (i > 0) {
						tag += ",";
					}
					tag += values[i];
				}
				tags += (tag+" ");
			}
			log.info("tags: " + tags);
		}
		
		String time = paramMap.get("amount")[0] + paramMap.get("unit")[0];
		
		model.addAttribute("max",Instant.now());
		
		Response r = api.getEventsRelative(paramMap.get("metric")[0], tags.trim(), time);
		model.addAttribute("events",r);
		
		return "fragments/parts :: events";
	}

}
