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
	private ArrayList<String> reverse_states;
	private ArrayList<String> reverse_ops;
	
	private int[][][] truth_table;
	
	private boolean order;
	
	OpsManager(){
		states = new HashMap<String,Integer>();
		ops = new HashMap<String,Integer>();
		reverse_states = new ArrayList<String>();
		reverse_ops = new ArrayList<String>();
		
		truth_table = null;
		
		order = false;;
	}
	
	OpsManager(boolean _order){
		states = new HashMap<String,Integer>();
		ops = new HashMap<String,Integer>();
		reverse_states = new ArrayList<String>();
		reverse_ops = new ArrayList<String>();
		order = _order;
		
		truth_table = null;
	}
	
	public void clear()
	{
		states = new HashMap<String,Integer>();
		ops = new HashMap<String,Integer>();
		reverse_states = new ArrayList<String>();
		reverse_ops = new ArrayList<String>();
		
		truth_table = null;
	}
	
	public int opInt(int op,int a, int b)
	{
		return this.truth_table[op][a][b];
	}
	
	public int opInt(String op,String a, String b)
	{
		int op_int = this.ops.get(op);
		int a_int = this.states.get(a);
		int b_int = this.states.get(b);
		
		return this.truth_table[op_int][a_int][b_int];
	}
	
	
	
	public String op(int op,int a, int b)
	{
		return this.reverse_states.get(this.truth_table[op][a][b]);
	}
	
	public String op(String op,String a, String b)
	{
		int op_int = this.ops.get(op);
		int a_int = this.states.get(a);
		int b_int = this.states.get(b);
		
		return this.reverse_states.get(this.truth_table[op_int][a_int][b_int]);
	}
	
	public String getStatus(int _status){
		return this.reverse_states.get(_status);
	}
	
	public int getStatus(String _status){
		return this.states.get(_status);
	}
	
	public String getOperations(int _status){
		return this.reverse_states.get(_status);
	}
	
	public int getOperations(String _status){
		return this.states.get(_status);
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
			this.reverse_states.add(j_states.get(i).getAsString());
			
		}
		
		// Collect the available operations 
		int i=0;
		for (Entry<String,JsonElement> item : j_ops.entrySet())
		{
			this.ops.put(item.getKey(), i);
			this.reverse_ops.add(item.getKey());
			i++;
		}
		// Initialize the truthtable
		int num_ops = this.reverse_ops.size();
		int num_states = this.reverse_states.size();
		this.truth_table = new int[num_ops][num_states][num_states];
		
		for (int[][] surface : this.truth_table) {
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
				this.truth_table[op_val][a_val][b_val] = x_val;
				if (!this.order)
				{
					this.truth_table[op_val][b_val][a_val] = x_val;
				}
			}
		}
		
		
		
	}
	
}
