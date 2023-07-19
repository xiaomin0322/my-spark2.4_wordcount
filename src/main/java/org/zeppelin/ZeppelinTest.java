package org.zeppelin;

import java.util.HashMap;
import java.util.Map;

import org.apache.zeppelin.client.ClientConfig;
import org.apache.zeppelin.client.ParagraphResult;
import org.apache.zeppelin.client.ZeppelinClient;

public class ZeppelinTest {

	public static void main(String[] args) throws Exception {
		ClientConfig clientConfig = new ClientConfig("http://X-usdp-fun03:28080");
		ZeppelinClient zClient = new ZeppelinClient(clientConfig);
		
		zClient.login("hadoop", "hadoop");
		
		String zeppelinVersion = zClient.getVersion();
		System.out.println("Zeppelin version: " + zeppelinVersion);
		// execute note 2A94M5J1Z paragraph by paragraph
		try {
			//ParagraphResult paragraphResult = zClient.executeParagraph("2HXQ8YRYN", "20230330-101251_25695257");
			ParagraphResult paragraphResult = zClient.executeParagraph("2HXQ8YRYN","20230330-101251_25695257");
			System.out.println("Execute the 1st spark tutorial paragraph, paragraph result: " + paragraphResult);
			/*paragraphResult = zClient.executeParagraph("2A94M5J1Z", "20150210-015302_1492795503");
			System.out.println("Execute the 2nd spark tutorial paragraph, paragraph result: " + paragraphResult);
			Map<String, String> parameters = new HashMap<>();
			parameters.put("maxAge", "40");
			paragraphResult = zClient.executeParagraph("2A94M5J1Z", "20150212-145404_867439529", parameters);
			System.out.println("Execute the 3rd spark tutorial paragraph, paragraph result: " + paragraphResult);
			parameters = new HashMap<>();
			parameters.put("marital", "married");
			paragraphResult = zClient.executeParagraph("2A94M5J1Z", "20150213-230422_1600658137", parameters);
*/			System.out.println("Execute the 4th spark tutorial paragraph, paragraph result: " + paragraphResult);
		
            System.out.println("");
		} finally {
			// you need to stop interpreter explicitly if you are running paragraph
			// separately.
			//zClient.stopInterpreter("2A94M5J1Z", "spark");
		}

	}
}
