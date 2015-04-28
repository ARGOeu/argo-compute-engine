package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;

import ops.OpsManagerTest;

import org.junit.BeforeClass;
import org.junit.Test;

public class RecalculationsTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", RecalculationsTest.class.getResource("/ops/recalc.json"));
	}
	
	@Test
	public void test() throws URISyntaxException, ParseException, IOException {
		//Prepare Resource File
		URL resJsonFile = RecalculationsTest.class.getResource("/ops/recalc.json");
		File jsonFile = new File(resJsonFile.toURI());
		
		Recalculations recMgr = new Recalculations();
		recMgr.loadJson(jsonFile);
		
		System.out.println(recMgr.check("NGI_GRNET", "GR-06-IASA", "2013-12-20"));
		
		
	}

}
