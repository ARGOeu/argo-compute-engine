package argo.flume.interceptor;

import org.junit.Assert;
import org.apache.commons.io.IOUtils;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;

import argo.flume.interceptor.DecodeInterceptor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

public class DecodeInterceptorTest {

	@Test
	public void TestIntercept() throws IOException, URISyntaxException {

		// Prepare Resource File
		URL resJsonFile = DecodeInterceptorTest.class.getResource("/event_body.json");
		File jsonFile = new File(resJsonFile.toURI());
		FileInputStream jsonFileStream = new FileInputStream(jsonFile);
		// Read the file contents into a string
		String jsonStr = IOUtils.toString(jsonFileStream, "UTF-8");

		// Create a new Flume Event object
		Event flumeEvent = EventBuilder.withBody(jsonStr.getBytes());

		// Prepare Builder to create a DecodeInterceptor Test Instance
		DecodeInterceptor.Builder bd = new DecodeInterceptor.Builder();
		DecodeInterceptor testDec = (DecodeInterceptor) bd.build();

		// Use Intercept method to produce output event
		Event output = testDec.intercept(flumeEvent);

		// This is the expected Message
		String expectedMsg = "this has to be decoded";
		// This is the output message body
		String outputMsg = new String(output.getBody());

		// Assert they are equal
		Assert.assertEquals(expectedMsg, outputMsg);

	}
}
