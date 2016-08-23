package posfeed.inventory.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
/*** Property file loader **/
public class PropertyFileLoader {
	
	private String configLocation;
	public static final String ENV_NAME = "LOCAL";
	public static Properties properties;
	
	public static Properties getProperties() {
		return properties;
	}
	
	private String buildPropertiesFileName() {
		String envName = System.getProperty(ENV_NAME);
		StringBuffer sb = new StringBuffer();
		if (envName == null || envName.trim().equals("")) {
			envName = "LOCAL";
		}
		sb.append(this.getConfigLocation() + "." + envName + ".properties");
		System.out.println("Filename:" + sb.toString());
		return sb.toString();
	}
	
	public String getConfigLocation() {
		return configLocation;
	}
	
	public void setConfigLocation(String configLocation) {
		this.configLocation = configLocation;
	}
	
	public void loadProperties() throws Exception {
		InputStream input = null;
		try {
			properties = new Properties();
			input = new FileInputStream(this.buildPropertiesFileName());
			properties.load(input);
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw e;
		}
	}
}
