package dynamiteDb;

public class ConfigFileEntry implements Comparable<ConfigFileEntry>{
	public String hexEncodedKeyValue;
	public String ipAddress;
	
	public ConfigFileEntry(){
		ipAddress="";
		hexEncodedKeyValue="";
	}
	
	public ConfigFileEntry(String ip, String hexKey){
		this.hexEncodedKeyValue=hexKey;
		this.ipAddress=ip;
	}
	
	
	public int compareTo(ConfigFileEntry s2){
        //here comes the comparison logic
   	int cmpValue= hexEncodedKeyValue.compareTo(s2.hexEncodedKeyValue);
   	return cmpValue;
	}
}