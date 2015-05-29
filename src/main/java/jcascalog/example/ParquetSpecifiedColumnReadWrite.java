package jcascalog.example;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import jcascalog.Api;
import jcascalog.Subquery;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import cascading.flow.FlowProcess;
import cascading.flow.FlowRuntimeProps;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.scheme.ParquetAvroScheme;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;



public class ParquetSpecifiedColumnReadWrite extends Configured implements Tool {	
	
	public int run(String[] args) throws Exception {		
		
		String input = args[0];
		String output = args[1];
		String queue = "default";
		if(args.length == 3)
		{
			queue = args[2];
		}
		
		
		Configuration hadoopConf = this.getConf();			
		
		hadoopConf.set(FlowRuntimeProps.GATHER_PARTITIONS, "4");
		hadoopConf.set("io.serializations", "cascading.kryo.KryoSerialization");
		hadoopConf.set("mapred.mapper.new-api", "false");
		hadoopConf.set("parquet.compression", "snappy");
		//hadoopConf.set("parquet.enable.dictionary", "true");
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
		
		Set<String> itemViewEventInputPathSet = new HashSet<>();
		
		FileStatus[] status = fs.listStatus(new Path(input));
		if (status != null) {
			for (int i = 0; i < status.length; i++) {
				if (status[i].isFile() && status[i].getPath().toString().endsWith("parquet")) {
					String parquetFile = status[i].getPath().toString();
					
					itemViewEventInputPathSet.add(parquetFile);
				}
			}
		}
		
		if(itemViewEventInputPathSet.size() == 0)
		{
			System.out.println("no parquet file exists!!!");
			
			return 0;
		}
				
		
	    // input parquet avro scheme.
		ParquetAvroScheme itemViewEventParquetScheme = new ParquetAvroScheme(new Schema.Parser().parse(getClass().getResourceAsStream("/META-INF/avro/item-view-event-specified.avsc")));
		itemViewEventParquetScheme.setSourceFields(new Fields("?base-properties", "?item-id"));		
		
		Tap[] parquetSourceTaps = new Tap[itemViewEventInputPathSet.size()];
		int i = 0;			
		for(String parquetFile : itemViewEventInputPathSet)
		{			
			parquetSourceTaps[i++] = new Hfs(itemViewEventParquetScheme, parquetFile);	
			System.out.println("included path: [" + parquetFile + "]");
		}		
		
		// multi-source tap.
		MultiSourceTap multiSourceTap = new MultiSourceTap(parquetSourceTaps);		
		
		// output parquet avro scheme.
		ParquetAvroScheme outParquetScheme = new ParquetAvroScheme(new Schema.Parser().parse(getClass().getResourceAsStream("/META-INF/avro/item-view-event-revised.avsc")));
		outParquetScheme.setSinkFields(new Fields("?revised-properties", "?service-id", "?item-id"));
		
		// sink tap.
		Tap outTap = new Hfs(outParquetScheme, output);	
	
		Subquery query = new Subquery("?revised-properties", "?service-id", "?item-id")
				.predicate(multiSourceTap, "?base-properties", "?item-id")
				.predicate(new Reconstruct(), "?base-properties").out("?service-id", "?revised-properties");
			
		
		Api.execute(outTap, query);
		
		return 0;
	}
	
	public static class Reconstruct extends CascalogFunction {
		
		@Override
		public void prepare(FlowProcess flowProcess, OperationCall operationCall) {					
		}

		@Override
		public void operate(FlowProcess flowProcess, FunctionCall fnCall) {	
		
			// baseProperties.
			Tuple t = (Tuple)fnCall.getArguments().getObject(0);	
			System.out.println("t: [" + t.toString() + "]");
			
			String serviceId = t.getString(0);
			
			String uid = t.getString(1);
			
			String pcid = t.getString(2);
			
			long timestamp = t.getLong(3);
			
			fnCall.getOutputCollector().add(new Tuple(serviceId, new Tuple(uid, pcid, timestamp)));
		}
	}	
	
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new ClassPathXmlApplicationContext("classpath*:/META-INF/spring/cascalog/*context.xml").getBean("hadoopConfiguration", Configuration.class);
		
		int exitCode = ToolRunner.run(conf, new ParquetSpecifiedColumnReadWrite(), args);
		System.exit(exitCode);
	}
}
