package dynamiteDb;
import java.util.HashMap;

import org.apache.commons.codec.binary.Hex;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.sql.Timestamp;


public class KeyValueStoreDriver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//hex encoded key
		String key="HEYYYY";
		MessageDigest hasher;
		byte[] hashVal;
		hashVal= new byte[3];
		try{
			hasher= MessageDigest.getInstance("SHA-256");
			hashVal=hasher.digest(key.getBytes());
		}
		catch(Exception e){
			e.printStackTrace();
		}
		String hexEncodedVal=Hex.encodeHexString(hashVal);
		
		HashMap<String,Integer> newVectClock= new HashMap<String,Integer>();
		newVectClock.put("Tylers-MBP-2/192.168.1.73", 51);
		newVectClock.put("HOL", 23);
		newVectClock.put("HOLL", 22);
		newVectClock.put("HOLAL", 6);
		
		Timestamp timeStamp = new Timestamp(877777888);
		
		String value= "VALLLLEE";
		
		KeyValueStore cmp = new KeyValueStore(hexEncodedVal,value,timeStamp,newVectClock);
		//boolean isEqual= cmp.isEqual(hey);
		//System.out.print(isEqual);
        InetAddress ip;
        try {
            ip = InetAddress.getLocalHost();
            cmp.updatePersistantStore();
            System.out.println(cmp.toString());
            //hostname = ip.getHostName();
            //System.out.println("Your current IP address : " + ip);
            //System.out.println("Your current Hostname : " + hostname);
 
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
	}
}
