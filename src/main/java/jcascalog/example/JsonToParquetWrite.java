package jcascalog.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import org.springframework.context.support.ClassPathXmlApplicationContext;

import cascading.flow.FlowRuntimeProps;
import cascading.scheme.ParquetAvroScheme;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;



public class JsonToParquetWrite extends Configured implements Tool {	
	
	public int run(String[] args) throws Exception {			
		
		String output = args[0];
		String queue = "default";
		if(args.length == 2)
		{
			queue = args[1];
		}
		
		
		Configuration hadoopConf = this.getConf();			
		
		hadoopConf.set(FlowRuntimeProps.GATHER_PARTITIONS, "4");
		hadoopConf.set("io.serializations", "cascading.kryo.KryoSerialization");
		hadoopConf.set("mapred.mapper.new-api", "false");
		hadoopConf.set("parquet.compression", "snappy");
		hadoopConf.set("parquet.enable.dictionary", "true");
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
		
		
		
		// input json 
		java.net.URL url = this.getClass().getResource("/data/event-sample.data");	
		BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));
		String strLine;		
		ObjectMapper mapper = new ObjectMapper();
		
		List rootList = new ArrayList();
		while ((strLine = br.readLine()) != null)   {				
			Map<String, Object> map = mapper.readValue(strLine, new TypeReference<Map<String, Object>>(){});
			Map<String, Object> basePropertiesMap = (Map<String, Object>)map.get("baseProperties");
			
			List list = new ArrayList();
			
			List basePropList = new ArrayList();
			basePropList.add(basePropertiesMap.get("eventType"));
			basePropList.add(basePropertiesMap.get("timestamp"));
			basePropList.add(basePropertiesMap.get("url"));
			basePropList.add(basePropertiesMap.get("referer"));
			basePropList.add(basePropertiesMap.get("uid"));
			basePropList.add(basePropertiesMap.get("pcid"));
			basePropList.add(basePropertiesMap.get("serviceId"));
			basePropList.add(basePropertiesMap.get("version"));
			basePropList.add(basePropertiesMap.get("deviceType"));
			basePropList.add(basePropertiesMap.get("domain"));
			basePropList.add(basePropertiesMap.get("site"));
			
			list.add(basePropList);
			list.add(map.get("itemId"));
			list.add(map.get("categoryId"));
			list.add(map.get("brandId"));
			list.add(map.get("itemType"));
			list.add(map.get("promotionId"));
			list.add(map.get("price"));
			list.add(map.get("itemTitle"));
			list.add(map.get("itemDescription"));
			list.add(map.get("thumbnailUrl"));
			
			
			rootList.add(list);			
		}
		
		br.close();
		
		String[] originFields = new String[] {"?base-properties", "?item-id", "!category-id", "!brand-id", "!item-type", "!promotion-id", "!price", "!item-title", "!item-description", "!thumbnail-url"};
		
		
		// output parquet avro scheme.
		ParquetAvroScheme outParquetScheme = new ParquetAvroScheme(new Schema.Parser().parse(getClass().getResourceAsStream("/META-INF/avro/item-view-event-origin.avsc")));
		outParquetScheme.setSinkFields(new Fields(originFields));
		
		// sink tap.
		Tap outTap = new Hfs(outParquetScheme, output);	
	
		Subquery query = new Subquery(originFields)
				.predicate(rootList, originFields);			
			
		
		Api.execute(outTap, query);
		
		return 0;
	}
	
	
	
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new ClassPathXmlApplicationContext("classpath*:/META-INF/spring/cascalog/*context.xml").getBean("hadoopConfiguration", Configuration.class);
		
		int exitCode = ToolRunner.run(conf, new JsonToParquetWrite(), args);
		System.exit(exitCode);
	}
}
