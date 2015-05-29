package jcascalog.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Test;

public class FilterItemViewEventTestSkip {
	
	@Test
	public void filter() throws Exception
	{
		FileInputStream fstream = new FileInputStream(new File("target/classes/data/event-sample.data"));
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		String strLine;		
		StringBuffer sb = new StringBuffer();
		ObjectMapper mapper = new ObjectMapper();
		while ((strLine = br.readLine()) != null)   {	
			if(strLine.contains("ITEM_VIEW_EVENT"))
			{
				Map<String, Object> map = mapper.readValue(strLine, new TypeReference<Map<String, Object>>(){});
				Map<String, Object> basePropertiesMap = (Map<String, Object>)map.get("baseProperties");
				basePropertiesMap.put("url", "http://any-url");
				
				map.put("baseProperties", basePropertiesMap);
				map.put("itemTitle", "any-title...");
				map.put("itemDescription", "any-description...");
				map.put("thumbnailUrl", "http://any-thumbnail.url...");
				
				String revisedStrLine = mapper.writeValueAsString(map);
				
				sb.append(revisedStrLine).append("\n");
			}
		}
		
		
		File file = new File("target/item-view-event.data");
		 
		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(sb.toString());
		bw.close();

		br.close();
	}

}
