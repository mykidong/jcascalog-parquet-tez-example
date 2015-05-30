package jcascalog.example;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import jcascalog.Api;
import jcascalog.Subquery;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import cascading.flow.FlowProcess;
import cascading.flow.FlowRuntimeProps;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.scheme.ParquetAvroScheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;


public class JsonToParquetWrite extends Configured implements Tool {	
	
	public int run(String[] args) throws Exception {			
		
		String input = args[0];
		String output = args[1];
		String defaultFS = args[2];
		String resourceManagerHost = args[3];
		String queue = "default";
		if(args.length == 5)
		{
			queue = args[4];
		}
		
		
		Configuration hadoopConf = this.getConf();			
		
		hadoopConf.set(FlowRuntimeProps.GATHER_PARTITIONS, "4");
		hadoopConf.set("io.serializations", "cascading.kryo.KryoSerialization");
		hadoopConf.set("mapred.mapper.new-api", "false");
		hadoopConf.set("parquet.compression", "snappy");
		hadoopConf.set("parquet.enable.dictionary", "true");
		hadoopConf.set("fs.defaultFS", defaultFS);
		hadoopConf.set("yarn.resourcemanager.hostname", resourceManagerHost);
		hadoopConf.set("tez.queue.name", queue);			

		// convert hadoop conf to map.
		Map<String, String> confMap = new HashMap<String, String>();
		Iterator<Entry<String, String>> iter = hadoopConf.iterator();
		while (iter.hasNext()) {
			Entry<String, String> entry = iter.next();
			confMap.put(entry.getKey(), entry.getValue());
		}		
		
	
		Api.setApplicationConf(confMap);			
		
		
		FileSystem fs = FileSystem.get(hadoopConf);		
		
		// first, delete output.
		fs.delete(new Path(output), true);			
	
		
		// source tap.
		Tap inTap = new Hfs(new TextLine(new Fields("?json")), input);
		
		
		String[] originFields = new String[] {"?base-properties", "?item-id", "!category-id", "!brand-id", "!item-type", "!promotion-id", "!price", "!item-title", "!item-description", "!thumbnail-url"};
		
		
		// output parquet avro scheme.
		ParquetAvroScheme outParquetScheme = new ParquetAvroScheme(new Schema.Parser().parse(getClass().getResourceAsStream("/META-INF/avro/item-view-event-origin.avsc")));
		outParquetScheme.setSinkFields(new Fields(originFields));
		
		// sink tap.
		Tap outTap = new Hfs(outParquetScheme, output);	
	
		Subquery query = new Subquery(originFields)
				.predicate(inTap, "?json")
				.predicate(new Parse(), "?json").out(originFields);		
			
		
		Api.execute(outTap, query);
		
		return 0;
	}
	
	
	public static class Parse extends CascalogFunction {
		
		private ObjectMapper mapper;
		
		@Override
		public void prepare(FlowProcess flowProcess, OperationCall operationCall) {	
			mapper = new ObjectMapper();
		}

		@Override
		public void operate(FlowProcess flowProcess, FunctionCall fnCall) {			
			String json = fnCall.getArguments().getString(0);	
			
			try
			{
				Map<String, Object> map = mapper.readValue(json, new TypeReference<Map<String, Object>>(){});
				Map<String, Object> basePropertiesMap = (Map<String, Object>)map.get("baseProperties");
				
				Tuple basePropTuple = new Tuple(basePropertiesMap.get("eventType"),
												basePropertiesMap.get("timestamp"),
												basePropertiesMap.get("url"),
												basePropertiesMap.get("referer"),
												basePropertiesMap.get("uid"),
												basePropertiesMap.get("pcid"),
												basePropertiesMap.get("serviceId"),
												basePropertiesMap.get("version"),
												basePropertiesMap.get("deviceType"),
												basePropertiesMap.get("domain"),
												basePropertiesMap.get("site"));
				
				
				fnCall.getOutputCollector().add(new Tuple(basePropTuple, 
															map.get("itemId"),
															map.get("categoryId"),
															map.get("brandId"), 
															map.get("itemType"), 
															map.get("promotionId"), 
															map.get("price"), 
															map.get("itemTitle"), 
															map.get("itemDescription"), 
															map.get("thumbnailUrl")));	
			
			} catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}	
	
	
	
	
	public static void main(String[] args) throws Exception
	{		
		int exitCode = ToolRunner.run(new Configuration(), new JsonToParquetWrite(), args);
		System.exit(exitCode);
	}
}
