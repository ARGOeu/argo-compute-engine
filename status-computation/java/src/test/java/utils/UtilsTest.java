package utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class UtilsTest {

	private State stateA[];
	private State stateB[];
	private State stateM[];
	private State state24h[];
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		
		stateA = new State[]{State.OK,		State.WARNING,	State.OK,		State.CRITICAL,	State.DOWNTIME	};
		stateB = new State[]{State.OK,		State.CRITICAL,	State.WARNING,	State.MISSING,	State.OK		};
		stateM = new State[]{State.MISSING,	State.MISSING,	State.MISSING,	State.MISSING,	State.MISSING	};
		
		state24h = new State[]{
				
				State.OK,		State.OK,		State.WARNING,	State.WARNING,	State.CRITICAL,	State.OK,
				State.OK,		State.CRITICAL,	State.UNKNOWN,	State.MISSING,	State.DOWNTIME,	State.DOWNTIME,
				State.OK,		State.OK,		State.OK,		State.OK,		State.WARNING,	State.UNKNOWN,
				State.OK,		State.WARNING,	State.OK,		State.OK,		State.DOWNTIME,	State.OK
				
		};		
		
	}

	@After
	public void tearDown() throws Exception {	
		stateA = null;
		stateB = null;
		stateM = null;
		state24h= null;
	}

	@Test
	public void testMakeMiss() {
		State[] temp = stateA.clone();
		Utils.makeMiss(temp);
		assertTrue(Arrays.equals(temp,stateM));
	}

	@Test
	public void testMakeOR() {
		State[] res = {State.OK,State.WARNING,State.OK,State.MISSING,State.OK};
		State[] temp = stateB.clone();
		
		try {
			Utils.makeOR(stateA, temp);
		} catch (IOException e) {
			assertNotNull(e.getMessage());
		}
		
		assertTrue(Arrays.equals(temp,res));
	}

	@Test
	public void testMakeAND() {
		State[] res = {State.OK,State.CRITICAL,State.WARNING,State.CRITICAL,State.DOWNTIME};
		State[] temp = stateB.clone();
		
		try {
			Utils.makeAND(stateA, temp);
		} catch (IOException e) {
			assertNotNull(e.getMessage());
		}
		
		assertTrue(Arrays.equals(temp,res));
		
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
		Map<Integer,Integer> myMap = new HashMap<Integer,Integer>();
		myMap.put(3, 6);
		Entry<Integer,Integer> myEntry =  myMap.entrySet().iterator().next();
		
		State [] tmp_timeline = new State[] 
				{	State.OK,State.CRITICAL,State.MISSING,State.DOWNTIME,
				 	State.OK,State.UNKNOWN,State.WARNING,State.CRITICAL,
				 	State.WARNING,State.MISSING,State.CRITICAL,State.OK
				};
	
		State [] result = new State[]
				{	State.OK,State.CRITICAL,State.MISSING,State.UNKNOWN,
			 	State.UNKNOWN,State.UNKNOWN,State.UNKNOWN,State.CRITICAL,
			 	State.WARNING,State.MISSING,State.CRITICAL,State.OK
				};
		
		Utils.putRecalculations(myEntry, tmp_timeline);
		assertTrue(Arrays.equals(tmp_timeline, result));
	}

	@Test
	public void testGetARReport() {
		Tuple myTuple = new DefaultTuple();
		Tuple resTuple = new DefaultTuple();
		myTuple.append(0.0);
		myTuple.append(0.0);
		myTuple.append(0.0);
		myTuple.append(0.0);
		myTuple.append(0.0);
		myTuple.append(0.0);
		
		/*double resA=76.190;
		double resR=88.889;
		double resUp=0.66667;
		double resD=0.12500;
		double resUn=0.12500;*/
		
		resTuple.append(76.190);
		resTuple.append(88.889);
		resTuple.append(0.66667);
		resTuple.append(0.12500);
		resTuple.append(0.12500);
		
		
		double quantum = (double)state24h.length;
		try {
			Utils.getARReport(state24h, myTuple, quantum);
			
			assertTrue(myTuple.get(0).equals(resTuple.get(0)));
			assertTrue(myTuple.get(1).equals(resTuple.get(1)));
			assertTrue(myTuple.get(2).equals(resTuple.get(2)));
			assertTrue(myTuple.get(3).equals(resTuple.get(3)));
			assertTrue(myTuple.get(4).equals(resTuple.get(4)));
			
		} catch (ExecException e) {
			
			e.printStackTrace();
		}		

	}

	@Test
	public void testGetTimeGroup() {
		String sDate = "15:30:43Z";
		int quantum=233;
		int res = Utils.getTimeGroup(sDate, quantum);
		assertTrue(res==155);
		
	}

	@Test
	public void testDetermineTimeGroup() {
		int resA,resB,resC;
		resA=resB=resC=0;
		String sDate="2014-04-01T15:30:43";
		int rtA=20140402;
		int rtB=20140312;
		int rtC=20140401;
		int quantum=233;
		
		try {
			resA=Utils.determineTimeGroup(sDate, rtA,quantum);
			resB=Utils.determineTimeGroup(sDate, rtB,quantum);
			resC=Utils.determineTimeGroup(sDate, rtC,quantum);	
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		assertTrue(resA==0);
		assertTrue(resB==232);
		assertTrue(resC==155);
			
		
	}

}
