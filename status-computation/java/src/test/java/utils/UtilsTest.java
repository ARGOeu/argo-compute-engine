package utils;

import static org.junit.Assert.*;


import java.io.IOException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.junit.Test;

import TestIO.JsonToPig;
import TestIO.TimetableReader;

public class UtilsTest {

	private static State t1[];
	private static State t2[];
	private static State t1ort2[];
	private static State t1andt2[];
	private static State t1miss[];
	private static State t1recalc[];
	

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Check if resource files exist
		assertNotNull("Test file missing", UtilsTest.class.getResource("/timetables/t1.json"));
		assertNotNull("Test file missing", UtilsTest.class.getResource("/timetables/t2.json"));
		assertNotNull("Test file missing", UtilsTest.class.getResource("/timetables/t1_or_t2.json"));
		assertNotNull("Test file missing", UtilsTest.class.getResource("/timetables/t1_and_t2.json"));
		assertNotNull("Test file missing", UtilsTest.class.getResource("/timetables/t1_miss.json"));
		assertNotNull("Test file missing", UtilsTest.class.getResource("/timetables/t1_recalc.json"));
		// Load json strings from files
		String t1_str =  IOUtils.toString(UtilsTest.class.getResourceAsStream("/timetables/t1.json"),"UTF-8");
		String t2_str =  IOUtils.toString(UtilsTest.class.getResourceAsStream("/timetables/t2.json"),"UTF-8");
		String t1ort2_str =  IOUtils.toString(UtilsTest.class.getResourceAsStream("/timetables/t1_or_t2.json"),"UTF-8");
		String t1andt2_str =  IOUtils.toString(UtilsTest.class.getResourceAsStream("/timetables/t1_and_t2.json"),"UTF-8");
		String t1miss_str =  IOUtils.toString(UtilsTest.class.getResourceAsStream("/timetables/t1_miss.json"),"UTF-8");
		String t1recalc_str =  IOUtils.toString(UtilsTest.class.getResourceAsStream("/timetables/t1_recalc.json"),"UTF-8");
		// Transform 
		t1 = TimetableReader.fromJson(t1_str);
		t2 = TimetableReader.fromJson(t2_str);
		t1ort2 = TimetableReader.fromJson(t1ort2_str);
		t1andt2 = TimetableReader.fromJson(t1andt2_str);
		t1miss = TimetableReader.fromJson(t1miss_str);
		t1recalc = TimetableReader.fromJson(t1recalc_str);
	
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {	
		// Nullify objects to be garbage collected
		t1 = null;
		t2 = null;
		t1ort2 = null;
		t1andt2 = null;
		t1miss = null;
		t1recalc = null;
	}

	@Test
	public void testMakeMiss() {
		
		State[] t1res = t1.clone();
		Utils.makeMiss(t1res);
		assertTrue(Arrays.deepEquals(t1res, t1miss));
	
	}

		
	@Test
	public void testMakeOR() throws IOException {
	
		State[] t2res = t2.clone();
		Utils.makeOR(t1, t2res);
		
		assertTrue(Arrays.deepEquals(t2res, t1ort2));
	}

	@Test
	public void testMakeAND() throws IOException {
		
		State[] t2res = t2.clone();
		Utils.makeAND(t1, t2res);
		
		assertTrue(Arrays.deepEquals(t2res,t1andt2));
	}

	
	@Test
	public void testRound() {
		
		double temp = 5.9454545;
		
		assertTrue(Utils.round(temp, 4, 0) == 5.9455);
		assertTrue(Utils.round(temp, 2, 1) == 5.94);
		assertTrue(Utils.round(temp, 1, 2) == 6.0);
		assertTrue(Utils.round(temp, 3, 3) == 5.945);
		assertTrue(Utils.round(temp, 4, 4) == 5.9455);
		assertTrue(Utils.round(temp, 2, 5) == 5.95);
		assertTrue(Utils.round(temp, 1, 6) == 5.9);
	}

	@Test
	public void testPutRecalculations() {
		
		State[] t1res = t1.clone();
		Map<Integer,Integer> spanMap = new HashMap<Integer,Integer>();
		spanMap.put(20, 46);
		Entry<Integer,Integer> span =  spanMap.entrySet().iterator().next();
		
		Utils.putRecalculations(span, t1res);
		
		assertTrue(Arrays.deepEquals(t1res, t1recalc));
		
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetARReport() throws ExecException {
		
		// { resA, resR, resUp, resD, resUn }
		Tuple report = JsonToPig.dblToTuple(new double[] {0.0, 0.0, 0.0, 0.0, 0.0});
		Tuple result = JsonToPig.dblToTuple(new double[] {50.0, 66.667, 0.33333, 0.33333, 0.16667});
		double quantum = (double)t1.length;
		
		Utils.getARReport(t1, report, quantum);
		
		assertTrue(report.compareTo(result) == 0);
		
	}

	@Test
	public void testGetTimeGroup() {
		
		String sDate = "15:30:43Z";
		int quantum=288; // Sampling Frequency - For 24h we sample on 5min intervals = 24*60 / 5 = 288 
		
		int res = Utils.getTimeGroup(sDate, quantum);
		
		assertTrue(res==186);
	}

	@Test
	public void testDetermineTimeGroup() throws IOException {
		int resA,resB,resC;
		resA=resB=resC=0;
		String sDate="2014-04-01T15:30:43";
		int rtA=20140402;
		int rtB=20140312;
		int rtC=20140401;
		int quantum=288; // Sampling Frequency - For 24h we sample on 5min intervals = 24*60 / 5 = 288 
		
		resA=Utils.determineTimeGroup(sDate, rtA,quantum);
		resB=Utils.determineTimeGroup(sDate, rtB,quantum);
		resC=Utils.determineTimeGroup(sDate, rtC,quantum);	
		
		assertTrue(resA==0);
		assertTrue(resB==287);
		assertTrue(resC==186);
	}

}
