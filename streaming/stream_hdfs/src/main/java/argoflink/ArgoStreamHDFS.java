package argoflink;

import java.io.File;
import java.io.IOException;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Base64;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;


// Flink Job : Stream metric data from Kafka Broker to HDFS
// job required cli parameters
// --zookeeper.connect {comma separated list of Zookeeper servers}
// --bootstrap.servers {comma separated list of Kafka servers}
// --group.id {consumer group}
// --schema {avro schema file to be used}
// --topic {kafka topic to connect to}
// --destination {hdfs destination}
public class ArgoStreamHDFS {
	// setup logger
	static Logger LOG = LoggerFactory.getLogger(ArgoStreamHDFS.class);
	
	
	
  public static void main(String[] args) throws Exception  {
	
	// Create flink execution enviroment
    StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
    
    // Initialize cli parameter tool
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);
    
   
    		 
    // Source: Kafka consumer - Listen to a (cli defined) topic name
	DataStream<String> messageStream = see
    		  .addSource(new FlinkKafkaConsumer09<String>( 
    		    parameterTool.getRequired("topic"), 
    		    new SimpleStringSchema(), 
    		    parameterTool.getProperties()));
    
	// Sink: Rolling sink - Listen to a (cli defined) hdfs destination
    RollingSink<String> rSink = new RollingSink<String>(parameterTool.getRequired("destination"));
    rSink.setBatchSize(1024);
    
    
    // Intermediate Transformation
    // Map function: kafka msg in json -> extract data field -> base64 decode -> avro decode -> pure payload string
    messageStream.rebalance().map(new MapFunction<String, String>() {
		private static final long serialVersionUID = -3841815159450875045L;

		@Override
		public String map(String value) throws IOException {
			JsonParser jsonParser = new JsonParser();
			// parse the json root object
			JsonElement jRoot = jsonParser.parse(value);
			// parse the json field "data" and read it as string
			// this is the base64 string payload
			String data = jRoot.getAsJsonObject().get("data").getAsString();
			// Decode from base64
			byte[] decoded64 = Base64.decodeBase64(data.getBytes("UTF-8"));
			// Decode from avro
			Schema avroSchema = new Schema.Parser().parse(new File(parameterTool.getRequired("avro-schema")));
			DatumReader<GenericRecord> avroReader = new SpecificDatumReader<GenericRecord>(avroSchema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(decoded64, null);
			GenericRecord payload2;
			payload2 = avroReader.read(null, decoder);
			// If not avro return the string
			if (payload2 != null) {
				return payload2.toString();
			} else {
				return Arrays.toString(decoded64);
			}
		}
	// Add rolling sink
    }).addSink(rSink);

    // Execute flink dataflow
    see.execute();
  }
  

}