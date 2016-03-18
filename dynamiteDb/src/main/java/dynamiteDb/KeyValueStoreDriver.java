package dynamiteDb;
import java.util.HashMap;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class KeyValueStoreDriver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		KeyValueStore cmp = new KeyValueStore("HII","OFhjbTIMESTAMPPP");
		HashMap<String,Integer> newMap= new HashMap<String,Integer>();
		newMap.put("Tylers-MBP-2/192.168.1.73", 38);
		newMap.put("HOL", 24);
		newMap.put("HOLL", 22);
		newMap.put("HOLAL", 6);
		cmp.setHashMap(newMap);
		//boolean isEqual= cmp.isEqual(hey);
		//System.out.print(isEqual);
        InetAddress ip;
        try {
            ip = InetAddress.getLocalHost();
            cmp.updatePersistantStore(ip.toString());
            System.out.println(cmp.toString());
            //hostname = ip.getHostName();
            //System.out.println("Your current IP address : " + ip);
            //System.out.println("Your current Hostname : " + hostname);
 
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
	}
}
