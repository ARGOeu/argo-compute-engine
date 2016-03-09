package argo.flume.interceptor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;


public class DecodeInterceptor implements Interceptor {

	// Prepare Logger
	private static final Logger LOG = Logger.getLogger(DecodeInterceptor.class);
	

	public static class Builder implements Interceptor.Builder {

		/**
		 * Called from builder class to build DecodeInterceptor
		 */
		@Override
		public Interceptor build() {
			return new DecodeInterceptor();
		}

		/**
		 * Called from builder class to grab flume configuration parameters
		 */
		@Override
		public void configure(Context flumeCtx) {

		}
	}

	/**
	 * Private constructor of class
	 * <p>
	 * Called from Builder 
	 */
	private DecodeInterceptor() {
	}

	/**
	 * Called when interceptor is initialized
	 * <p>
	 * Just log info that interceptor was initialized 
	 */
	@Override
	public void initialize() {
		LOG.info("Interceptor Initialized");
	}

	/**
	 * Intercepts an event from flume pipeline 
	 * <p>
	 * This method is called for each signle event that interceptor picks up
	 * 
	 * @param event
	 *            An Event object containing the flume event
	 */
	@Override
	public Event intercept(Event event) {
	
		try {
			// Grab the event body = actual json message from broker network
			String body = new String(event.getBody());
			// create a new parser for json msg
			JsonParser jsonParser = new JsonParser();
			// parse the json root object
			JsonElement jRoot = jsonParser.parse(body);
			// parse the json field "data" and read it as string
			// this is the base64 string payload
			String data = jRoot.getAsJsonObject().get("data").getAsString();
			// decode the base64 payload into it's original form
			byte[] decoded =  Base64.decodeBase64(data.getBytes("UTF-8"));
			// replace the whole event body with the newly decoded bytes
			event.setBody(decoded);
	
		} catch (Exception e) {
			// On exception log the message 
			// this usually goes to /var/log/flume/
			LOG.warn(e.getMessage());
			return null;
		}
		
		// return the event to flume pipeline
		return event;
	}

	/**
	 * Intercepts an batch of events from flume pipeline
	 * <p>
	 * This method is called for a batch(list) of events captured by interceptor
	 * 
	 * @param events
	 *            A List of Event objects containing the flume events captured
	 */
	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> passEvents = new ArrayList<Event>();

		for (Event event : events) {
			Event handledEvent = intercept(event);

			if (handledEvent != null) {
				passEvents.add(handledEvent);
			}
		}
		return passEvents;
	}

	
	/**
	 * Called when interceptor is closed
	 * <p>
	 * Just log info that interceptor was closed
	 */
	@Override
	public void close() {
		LOG.info("Interceptor closed");
	}

}
