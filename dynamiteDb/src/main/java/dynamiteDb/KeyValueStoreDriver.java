package dynamiteDb;
import java.io.FileInputStream;
import java.util.HashMap;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.ObjectInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class KeyValueStoreDriver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		KeyValueStore hey = new KeyValueStore("HII","AFASFASFSA");
		KeyValueStore cmp = new KeyValueStore("HII","OF TIMESTAMP");
		HashMap<String,Integer> newMap= new HashMap<String,Integer>();
		newMap.put("Tylers-MBP-2/192.168.1.73", 19);
		newMap.put("HOL", 18);
		newMap.put("HOLL", 9);
		newMap.put("HOLAL", 5);
		cmp.setHashMap(newMap);
		//boolean isEqual= cmp.isEqual(hey);
		//System.out.print(isEqual);
        InetAddress ip;
        String hostname;
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
		/*
		try {
			MessageDigest hasher= MessageDigest.getInstance("SHA-256");
			String fileName= hasher.digest("HI".getBytes()).toString();
			FileInputStream fileIn = new FileInputStream("src/main/resources/"+"HI"+".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			KeyValueStore cmp= (KeyValueStore) in.readObject();
			in.close();
			fileIn.close();
			boolean isEqual= cmp.isEqual(hey);
			System.out.print(isEqual);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}

}
