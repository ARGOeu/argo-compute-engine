package ops;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class OpsManager {
	
	private HashMap<String,Integer> states;
	private HashMap<String,Integer> ops;
	private ArrayList<String> revStates;
	private ArrayList<String> revOps;
	
	private int[][][] truthTable;
	
	private boolean order;
	
	OpsManager(){
		this.states = new HashMap<String,Integer>();
		this.ops = new HashMap<String,Integer>();
		this.revStates = new ArrayList<String>();
		this.revOps = new ArrayList<String>();
		
		this.truthTable = null;
		
		this.order = false;;
	}
	
	OpsManager(boolean _order){
		this.states = new HashMap<String,Integer>();
		this.ops = new HashMap<String,Integer>();
		this.revStates = new ArrayList<String>();
		this.revOps = new ArrayList<String>();
		this.order = _order;
		
		this.truthTable = null;
	}
	
	public void clear()
	{
		this.states = new HashMap<String,Integer>();
		this.ops = new HashMap<String,Integer>();
		this.revStates = new ArrayList<String>();
		this.revOps = new ArrayList<String>();
		
		this.truthTable = null;
	}
	
	public int opInt(int op,int a, int b)
	{
		return this.truthTable[op][a][b];
	}
	
	public int opInt(String op,String a, String b)
	{
		int opInt = this.ops.get(op);
		int aInt = this.states.get(a);
		int bInt = this.states.get(b);
		
		return this.truthTable[opInt][aInt][bInt];
	}
	
	
	
	public String op(int op,int a, int b)
	{
		return this.revStates.get(this.truthTable[op][a][b]);
	}
	
	public String op(String op,String a, String b)
	{
		int opInt = this.ops.get(op);
		int aInt = this.states.get(a);
		int bInt = this.states.get(b);
		
		return this.revStates.get(this.truthTable[opInt][aInt][bInt]);
	}
	
	public String getStatus(int status){
		return this.revStates.get(status);
	}
	
	public int getStatus(String status){
		return this.states.get(status);
	}
	
	public String getOperation(int op){
		return this.revOps.get(op);
	}
	
	public int getOperation(String op){
		return this.ops.get(op);
	}

	public ArrayList<String> availableStates()
	{
		
		return this.revStates;
	}
	
	public ArrayList<String> availableOps()
	{
		return this.revOps;
	}
	
	
	
	public void openFile(File json_file) throws FileNotFoundException{
		// Clear data
		this.clear();
		
		BufferedReader br = new BufferedReader(new FileReader(json_file));
		JsonParser json_parser = new JsonParser();
		JsonElement j_element = json_parser.parse(br);
		JsonObject j_obj = j_element.getAsJsonObject();
		JsonArray j_states = j_obj.getAsJsonArray("states");
		JsonObject j_ops = j_obj.getAsJsonObject("operations");
		
		
		// Collect the available states
		for (int i=0;i<j_states.size();i++)
		{
			this.states.put(j_states.get(i).getAsString(),i);
			this.revStates.add(j_states.get(i).getAsString());
			
		}
		
		// Collect the available operations 
		int i=0;
		for (Entry<String,JsonElement> item : j_ops.entrySet())
		{
			this.ops.put(item.getKey(), i);
			this.revOps.add(item.getKey());
			i++;
		}
		// Initialize the truthtable
		int num_ops = this.revOps.size();
		int num_states = this.revStates.size();
		this.truthTable = new int[num_ops][num_states][num_states];
		
		for (int[][] surface : this.truthTable) {
	        for (int[] line : surface) {
	            Arrays.fill(line, -1);
	        }
	    }
		
		// Fill the truth table
		for (Entry<String,JsonElement>item : j_ops.entrySet())
		{
			String opname = item.getKey();
			JsonArray tops = item.getValue().getAsJsonArray();
			//System.out.println(tops);
		
			for (int j=0;j<tops.size();j++)
			{
				//System.out.println(opname);
				JsonObject row = tops.get(j).getAsJsonObject();
				
				int a_val = this.states.get(row.getAsJsonPrimitive("A").getAsString());
				int b_val = this.states.get(row.getAsJsonPrimitive("B").getAsString());
				int x_val = this.states.get(row.getAsJsonPrimitive("X").getAsString());
				int op_val = this.ops.get(opname);
				
				//Fill in truth table
				// Check if order sensitivity is off so to insert two truth values 
				// ...[a][b] and [b][a]
				this.truthTable[op_val][a_val][b_val] = x_val;
				if (!this.order)
				{
					this.truthTable[op_val][b_val][a_val] = x_val;
				}
			}
		}
		
		
		
	}
	
}
