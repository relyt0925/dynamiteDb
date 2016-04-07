package dynamiteDb;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.codec.binary.Hex;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Timer;

/**
 * This class instantiates the Key Value Server. Server initialization is peformed in 
 * this class
 * @author Satyajeet
 *
 */
public class KeyValueServer {
	
	public final static String METHOD_GET = "GET";
	public final static String METHOD_PUT = "PUT";
	public final static String METHOD_ANTI_ENTROPY = "ANTI_ENTROPY";
	private static final String[] ipAddressList=
		{"52.201.0.131","52.200.255.102","52.200.254.246","52.200.248.223","52.200.241.247" };
	private static final String path="src/main/resources/systemips/";

	/**
     * Application method to start the server. The server runs in an infinite loop
     * listening on port 9090.  When a connection is requested, it
     * spawns a new thread to do the servicing and immediately returns
     * to listening.  
     */
	public static void main(String[] args) throws Exception{
		//boolean hey=true;
		HashMap<String,ReadWriteLock> initKeyToLockMap= getListOfKeyFiles();
		ClientListener.setInitKeyLockHashmap(initKeyToLockMap);
		/*
		if(hey){
			ConfigFileEntry[] replicaTrack=generateReplicaTracker();
			for(int i=0;i<replicaTrack.length;i++){
				System.out.println(replicaTrack[i].hexEncodedKeyValue+" "+replicaTrack[i].ipAddress);
				InetAddress a= InetAddress.getByName(replicaTrack[i].ipAddress);
				System.out.println(a.toString());
			}
			return;
		}
		*/
		ConfigFileEntry[] replicaTrack=generateReplicaTracker();
		ClientListener.setReplicaTracker(replicaTrack);
		
		ServerSocket listener = new ServerSocket(13000);
		Timer timer = new Timer();
		DaemonServicesHandler daemonHandler = new DaemonServicesHandler();
		daemonHandler.addDaemonService(new AntiEntopyDeamon(30));
		timer.scheduleAtFixedRate(new DaemonServicesHandler(), 1000, 1000);

	        try {
	            while (true) {
	                new ClientListener(listener.accept()).start();
	            }
	        } finally {
	            listener.close();
	        }
	}
	
	private static HashMap<String,ReadWriteLock> getListOfKeyFiles(){
		File folder = new File("src/main/resources/keys");
		File[] listOfFiles = folder.listFiles();
		HashMap<String,ReadWriteLock> initKeyToLockMap = new HashMap<String,ReadWriteLock>();
		for(File i : listOfFiles){
			System.out.println(i.getName().substring(0, i.getName().lastIndexOf('.')));
			String hexKey=i.getName().substring(0, i.getName().lastIndexOf('.'));
			initKeyToLockMap.put(hexKey, new ReentrantReadWriteLock());
		}
		return initKeyToLockMap;
	}
	
	private static String getPublicIp(){
		try{
			byte[] encoded = Files.readAllBytes(Paths.get("src/main/resources/ip/hostIp.conf"));
			String ip= new String(encoded,StandardCharsets.US_ASCII);
			return ip;
		}
		catch(IOException e){
			e.printStackTrace();
		}
		return "";
	}
	
	private static ConfigFileEntry[] generateReplicaTracker(){
		ConfigFileEntry[] configArray= new ConfigFileEntry[ipAddressList.length];
		for(int i=0;i<ipAddressList.length;i++){
			try{
				MessageDigest hasher= MessageDigest.getInstance("SHA-256");
				byte[] hashVal=hasher.digest(ipAddressList[i].getBytes());
				String hexEncodedVal=Hex.encodeHexString(hashVal);
				//System.out.println(hexEncodedVal);
				ConfigFileEntry a = new ConfigFileEntry();
				a.hexEncodedKeyValue=hexEncodedVal;
				a.ipAddress=ipAddressList[i];
				configArray[i]=a;
			}
			catch(NoSuchAlgorithmException e){
				e.printStackTrace();
			}
		}
		Arrays.sort(configArray);
		String myIp=getPublicIp();
		int foundIndex=0;
		for(int i=0;i<configArray.length;i++){
			//match on ips
			if(myIp.toString().compareTo(configArray[i].ipAddress)==0){
				foundIndex=i;
				break;
			}
		}
		//set to 3 because 2 replicas
		ConfigFileEntry[] replicaTracker= new ConfigFileEntry[3];
		for(int i=0;i<3;i++){
			int indexer=(foundIndex+i)%configArray.length;
			//System.out.println(indexer);
			ConfigFileEntry entry = new ConfigFileEntry(configArray[indexer].ipAddress,configArray[indexer].hexEncodedKeyValue);
			replicaTracker[i]=entry;
		}
		return replicaTracker;			
	}
	
	private static void generateConfigFile(){
		ConfigFileEntry[] configArray= new ConfigFileEntry[ipAddressList.length];
		for(int i=0;i<ipAddressList.length;i++){
			try{
				MessageDigest hasher= MessageDigest.getInstance("SHA-256");
				byte[] hashVal=hasher.digest(ipAddressList[i].getBytes());
				String hexEncodedVal=Hex.encodeHexString(hashVal);
				//System.out.println(hexEncodedVal);
				ConfigFileEntry a = new ConfigFileEntry();
				a.hexEncodedKeyValue=hexEncodedVal;
				a.ipAddress=ipAddressList[i];
				configArray[i]=a;
			}
			catch(NoSuchAlgorithmException e){
				e.printStackTrace();
			}
		}
		Arrays.sort(configArray);
		//write out config array
		try{
			FileWriter writer = new FileWriter(path+"systemips.conf");
			for(int i=0;i<ipAddressList.length;i++){
				writer.append(configArray[i].hexEncodedKeyValue);
				writer.append(",");
				writer.append(configArray[i].ipAddress);
				writer.append("\n");
			}
			writer.flush();
			writer.close();
		}
		catch(IOException e){
			e.printStackTrace();
		}
		return;
	}
}
