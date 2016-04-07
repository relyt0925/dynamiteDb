package dynamiteDb;

/**
 * This class stores key range data and ips about the current node and the replicas of 
 * the current node's primary key range.
 * @author tylerlisowski
 *
 */
public class ConfigFileEntry implements Comparable<ConfigFileEntry>{
	/**
	 * hexEncodedKeyValue- hex encoded value of the hash of the key (key is ip)
	 */
	public String hexEncodedKeyValue;
	/**
	 * ipAddress- ip address corresponding to the hash value
	 */
	public String ipAddress;
	
	public ConfigFileEntry(){
		ipAddress="";
		hexEncodedKeyValue="";
	}
	
	public ConfigFileEntry(String ip, String hexKey){
		this.hexEncodedKeyValue=hexKey;
		this.ipAddress=ip;
	}
	
	/**
	 * compareTo- compares two hash values to see which one is greater. Used for sorting.
	 */
	public int compareTo(ConfigFileEntry s2){
        //here comes the comparison logic
   	int cmpValue= hexEncodedKeyValue.compareTo(s2.hexEncodedKeyValue);
   	return cmpValue;
	}
}