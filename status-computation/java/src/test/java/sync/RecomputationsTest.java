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

public class RecomputationsTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", RecomputationsTest.class.getResource("/ops/recomp.json"));
	}

	@Test
	public void test() throws URISyntaxException, ParseException, IOException {
		// Prepare Resource File
		URL resJsonFile = RecomputationsTest.class.getResource("/ops/recomp.json");
		File jsonFile = new File(resJsonFile.toURI());

		Recomputations recMgr = new Recomputations();
		recMgr.loadJson(jsonFile);

		assertEquals(recMgr.shouldRecompute("GR-01-AUTH", "2013-12-09"), true);
		assertEquals(recMgr.shouldRecompute("GR-01-AUTH", "2013-12-10"), true);
		assertEquals(recMgr.shouldRecompute("GR-01-AUTH", "2013-12-08"), true);
		// should return false because date is out of recomputation period
		assertEquals(recMgr.shouldRecompute("GR-01-AUTH", "2013-12-07"), false);
		assertEquals(recMgr.shouldRecompute("GR-01-AUTH", "2013-12-11"), false);
		assertEquals(recMgr.shouldRecompute("GR-01-AUTH", "2013-08-02"), false);
		// should return false because site is out of exclude list
		assertEquals(recMgr.shouldRecompute("GR-04-IASA", "2013-12-09"), false);
		assertEquals(recMgr.shouldRecompute("GR-04-IASA", "2013-12-10"), false);
		assertEquals(recMgr.shouldRecompute("GR-04-IASA", "2013-12-08"), false);
		// should return false because NGI doesn't belong in the recomputation
		// request
		assertEquals(recMgr.shouldRecompute("SITEA", "2013-12-09"), false);
		assertEquals(recMgr.shouldRecompute("SITEB", "2013-12-10"), false);
		assertEquals(recMgr.shouldRecompute("SITEC", "2013-12-08"), false);

	}

}
