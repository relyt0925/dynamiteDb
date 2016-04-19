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
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.codec.binary.Hex;

import java.util.Arrays;
import java.util.Date;
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
	public final static String LOG = "Log";
	public final static int numReplicas=2;
	/**
	 * ipAddressList- List of IPs of all database nodes
	 */
	private static final String[] ipAddressList=
		{"52.201.0.131","52.200.255.102","52.200.254.246","52.200.248.223","52.200.241.247" };
	/**
	 * path- directory that will store config file with all IPs and hash values
	 */
	private static final String path="src/main/resources/systemips/";

	/**
     * Application method to start the server. The server runs in an infinite loop
     * listening on port 13000.  When a connection is requested, it
     * spawns a new thread to do the servicing and immediately returns
     * to listening.  
     */
	public static void main(String[] args) throws Exception{
		//initialize KeyLockHashmap
		generateConfigFile();
		HashMap<String,ReadWriteLock> initKeyToLockMap= getListOfKeyFiles();
		ClientListener.setInitKeyLockHashmap(initKeyToLockMap);
		Logger logger = Logger.getLogger(LOG);
		FileHandler fh = new FileHandler("Logs/"+new Date().getTime()+".log");  
	    logger.addHandler(fh);
	    SimpleFormatter formatter = new SimpleFormatter();  
	    fh.setFormatter(formatter); 
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
		//initalize replica information
		ConfigFileEntry[] replicaTrack=generateReplicaTracker();
		ClientListener.setReplicaTracker(replicaTrack);
		//setup socket and anti-entropy process
		ServerSocket listener = new ServerSocket(13000);
		Timer timer = new Timer();
		DaemonServicesHandler daemonHandler = new DaemonServicesHandler();
		daemonHandler.addDaemonService(new AntiEntopyDeamon(30));
		daemonHandler.addDaemonService(new StatsDaemon(10));
		timer.scheduleAtFixedRate(new DaemonServicesHandler(), 1000, 1000);

	        try {
	            while (true) {
	                new ClientListener(listener.accept()).start();
	            }
	        } finally {
	            listener.close();
	        }
	}
	
	/**
	 * getListOfKeyFiles- reads persistent store to initalize keyLock structure
	 * with keys that are in the system
	 * @return initKeyToLockMap- inital keyToLock Structure
	 */
	private static HashMap<String,ReadWriteLock> getListOfKeyFiles(){
		File folder = new File("src/main/resources/keys");
		File[] listOfFiles = folder.listFiles();
		HashMap<String,ReadWriteLock> initKeyToLockMap = new HashMap<String,ReadWriteLock>();
		for(File i : listOfFiles){
			//System.out.println(i.getName().substring(0, i.getName().lastIndexOf('.')));
			String hexKey=i.getName().substring(0, i.getName().lastIndexOf('.'));
			if(hexKey.isEmpty()){
				continue;				
			}
			initKeyToLockMap.put(hexKey, new ReentrantReadWriteLock());
		}
		return initKeyToLockMap;
	}
	
	/**
	 * getPublicIp- gets public IP of node from file (file provided by AWS)
	 * @return ip- ipAddress of the node
	 */
	private static String getPublicIp(){
		try{
			byte[] encoded = Files.readAllBytes(Paths.get("src/main/resources/ip/hostIp.conf"));
			String ip= new String(encoded,StandardCharsets.US_ASCII);
			//System.out.println("IP FROM FILE IS:" +ip);
			return ip;
		}
		catch(IOException e){
			e.printStackTrace();
		}
		return "";
	}
	
	/**
	 * generateReplicaTracker- generates replica key range and routing data for node
	 * @return replicaTracker- key range and routing data for the replica nodes
	 */
	private static ConfigFileEntry[] generateReplicaTracker(){
		ConfigFileEntry[] configArray= new ConfigFileEntry[ipAddressList.length];
		for(int i=0;i<ipAddressList.length;i++){
			try{
				//hash ip and add to unsorted list
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
		//String myIp = "52.201.0.131";
		int foundIndex=0;
		//find where IP is in the sorted array
		for(int i=0;i<configArray.length;i++){
			//match on ips
			if(myIp.toString().compareTo(configArray[i].ipAddress)==0){
				foundIndex=i;
				//System.out.println("FOUND INDEX: "+Integer.toString(foundIndex));
				break;
			}
		}
		//set to 3 because 2 replicas
		//and now find replicas (the next entries does mod to return back to start if need be)
		ConfigFileEntry[] replicaTracker= new ConfigFileEntry[ipAddressList.length];
		//NOTE: THIS ONLY WORKS FOR CASE OF 5 with replication factor of 3
		//IT WILL JUST PUT THE ENTRIES IN THE 
		//System.out.println("STARTING REPLICA CREATOR");
		for(int i=0;i<numReplicas;i++){
			//find my secondary keyset, and tietary keyset
			int indexer=(foundIndex-(i+1));
			if(indexer<0){
				indexer=ipAddressList.length+indexer;
			}
			//System.out.println("INDEXER: "+Integer.toString(indexer));
			//System.out.println(configArray[indexer].ipAddress);
			replicaTracker[numReplicas-1-i]= new ConfigFileEntry(configArray[indexer].ipAddress,configArray[indexer].hexEncodedKeyValue);
		}
		//insert my node into the proper spot
		//System.out.println(configArray[numReplicas].ipAddress);
		replicaTracker[numReplicas]= new ConfigFileEntry(configArray[foundIndex].ipAddress,configArray[foundIndex].hexEncodedKeyValue);
		//get nodes greater than my replica
		//System.out.println("GREATER INDEXER");
		for(int i=numReplicas+1;i<((2*numReplicas)+1);i++){
			int indexer=(foundIndex+(i-numReplicas))%configArray.length;
			//System.out.println("INDEXER: "+Integer.toString(indexer));
			//System.out.println(indexer);
			ConfigFileEntry entry = new ConfigFileEntry(configArray[indexer].ipAddress,configArray[indexer].hexEncodedKeyValue);
			replicaTracker[i]=entry;
		}
		System.out.println("END REPLICA CREATOR");
		for(int i=0;i<replicaTracker.length;i++){
			System.out.println(replicaTracker[i].ipAddress+"  "+ replicaTracker[i].hexEncodedKeyValue);
		}
		return replicaTracker;			
	}
	/**
	 * generateConfigFile- generates configuration file of all nodes and hashes of IPs in the system
	 */
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
