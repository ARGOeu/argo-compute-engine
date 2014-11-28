package myudf;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class ProfileSupport extends FilterFunc {

	
	private static String mongo_host;
	private static int mongo_port;
	private Map<String , String> active_profiles = null;
    
	public ProfileSupport(String inp_mongo_host, String inp_mongo_port){
		mongo_host = inp_mongo_host;
		mongo_port = Integer.parseInt(inp_mongo_port);
	}
	
	public ProfileSupport() {
    }
	
	public void initActiveProfiles() throws UnknownHostException{
		// Initialize hashmap structure
		active_profiles = new HashMap<String, String>();
		// Connect to the database and get site information
		MongoClient mongoClient = new MongoClient(mongo_host, mongo_port);
		DB db = mongoClient.getDB( "AR" );
		DBCollection coll = db.getCollection("active_profiles");
		DBCursor cursor = coll.find();
		DBObject item;
		while(cursor.hasNext()) {
				item = cursor.next();
				active_profiles.put((String)item.get("p"),"yes");
		}		
		
	}
	
	@Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        
        if (active_profiles == null){
        	initActiveProfiles();
        }
        
        if (active_profiles.get((String)input.get(0)) != null) {
        	return true;
        } 
        
        return null;
        
    }

	
	
}
