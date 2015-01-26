package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;

public class GroupsOfGroupsTest {

	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", GroupsOfGroupsTest.class.getResource("/avro/group_groups_test.avro"));
	}
	
	@Test
	public void test() throws URISyntaxException, IOException {
		//Prepare Resource File
		URL resAvroFile = EndpointGroupsTest.class.getResource("/avro/group_groups_test.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instatiate class
		GroupsOfGroups gg = new GroupsOfGroups();
		// Test loading file
		gg.loadAvro(avroFile);
		assertNotNull("File Loaded",gg);
		
		// Test retrieve group by subgroup name and group type
		assertEquals(gg.getGroup("NGI", "UNI-BONN"),"NGI_DE");
		assertEquals(gg.getGroup("NGI", "MSFG-OPEN"),"NGI_FRANCE");
		assertEquals(gg.getGroup("NGI", "HG-02-IASA"),"NGI_GRNET");
		assertEquals(gg.getGroup("NGI", "NIIFI_SC"),"NGI_HU");
		
	}

}
