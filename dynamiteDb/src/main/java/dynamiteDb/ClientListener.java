package dynamiteDb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.sql.Timestamp;
import org.json.JSONException;
import org.json.JSONObject;
/**
 * This class extends the java.lang.Thread class and handles all the processing
 * related to client request
 * 
 * @author Satyajeet
 *
 */
public class ClientListener extends Thread {
	/**
	 * socket- socket that is passed to thread that has client connection
	 */
	private Socket socket;
	/**
	 * keyLockMap- maps keys to a read write lock to handle access to persistant storage
	 * only one writer but multiple readers allowed
	 */
	public static HashMap <String,ReadWriteLock> keyLockMap;
	/**
	 * replicaTracker- (Hash(ip), ip) tuple. First index is local node, and the next 2 are the two replicas.
	 * Replicas ordered by key.
	 */
	public static ConfigFileEntry[] replicaTracker;
	/**
	 * keyLockMapLock- to ensure only one writer can update the keyLockMap at a time.
	 * Only done when new key enters system.
	 */
	public static final ReadWriteLock keyLockMapLock= new ReentrantReadWriteLock();
	/**
	 * resPath- path where keys will be serialized
	 */
	private final String resPath="src/main/resources/keys/";
	/**
	 * publicIpFilePath- path to file that stores public ip
	 */
	private final String publicIpFilePath="src/main/resources/ip/hostIp.conf";

	/**
	 * Constructor
	 * @param socket- client connection (already established)
	 */
	public ClientListener(Socket socket) {
		this.socket = socket;
	}
	
	/**
	 * getPublicIp- reads public IP from file fetched from AWS
	 * @return public IP of node
	 * @throws IOException - if File cannot be found/read
	 */
	private String getPublicIp() throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(publicIpFilePath));
		String ip= new String(encoded,StandardCharsets.US_ASCII);
		return ip;
	}

	/**
	 * Services this thread's client request.
	 */
	@Override
	public void run() {
		try {
			// Read the byte array from the socket
			BufferedReader in = new BufferedReader(new InputStreamReader(
					socket.getInputStream()));

			// Get messages from the client
			String input = in.readLine();
			JSONObject jsonObj = new JSONObject(input);
			
			//Based upon the method perform call appropriate handling function
			String method = jsonObj.getString("METHOD");
			
			
			//Based upon the method call appropriate handler function
			if(KeyValueServer.METHOD_GET.equals(method)){
				handleGETRequest(jsonObj);
				return;
			}
			if(KeyValueServer.METHOD_PUT.equals(method)){
				handlePUTReq(jsonObj);
				return;
			}
			if(KeyValueServer.METHOD_ANTI_ENTROPY.equals(method)){
				handleANTI_ENTROPYReq(jsonObj);
				return;
			}
			
			//Debug Purpose only
			//System.out.println("KEY = "+key + "; METHOD = "+method+"; VALUE = "+value);
			//System.out.println(Arrays.asList(vectorMap)); 
		} catch (IOException e) {
			Logger.getLogger("DynamiteDB").log(Level.SEVERE,
					"error while reading data from client " + e);
			e.printStackTrace();
		} catch (JSONException e) {
			Logger.getLogger("DynamiteDB").log(Level.SEVERE,
					"incorrect json formed " + e);
			e.printStackTrace();
		} finally {
			try {
				socket.close();
			} catch (IOException e) {
				Logger.getLogger("DynamiteDB").log(Level.SEVERE,
						"Couldn't close a socket");
			}
		}
	}

	
	
	/**
	 * Function to handle anti entropy request. Serializes data and sends the updated
	 * version back to the client.
	 * @param jsonObj- data for key sent from other datanode
	 * @throws JSONException 
	 */
	private void handleANTI_ENTROPYReq(JSONObject jsonObj) throws JSONException{
		//Extract data from json object and form key value store
		String key = jsonObj.getString("KEY");
		String value = jsonObj.getString("VALUE");
		System.out.println(jsonObj.getString("TIMESTAMP"));
		Timestamp time= Timestamp.valueOf(jsonObj.getString("TIMESTAMP"));
		JSONObject vectorClockJSON = jsonObj.getJSONObject("VECTOR_CLOCK");
		HashMap<String,Integer> vectClock=convertVectorClockFromJSON(vectorClockJSON);
		KeyValueStore newData= new KeyValueStore(key,value,time,vectClock);
		//get write lock and do serialization process
		keyLockMapLock.writeLock().lock();
		//if no key in map, key doesn't exist on disk so add it to the KeyLockMap
		if(!keyLockMap.containsKey(key)){
			keyLockMap.put(key,new ReentrantReadWriteLock());
		}
		keyLockMapLock.writeLock().unlock();
		//now that key in map, acquire its write lock so entire map structure can be accessed by
		//other threads
		keyLockMap.get(key).writeLock().lock();
		try {
            //InetAddress ip = InetAddress.getLocalHost();
			//update persistant storage and note that it is anti-entropy process
			//(no incrementing vector clock) also note ip doesnt matter since anti-entropy
			//String ip=getPublicIp();
            newData.updatePersistantStore();
            System.out.println(newData.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
		keyLockMap.get(key).writeLock().unlock();
		sendKeyValueStoreObject(newData);	
	}
	
	
	
	/**
	 * convertVectorClockFromJSON- converts JSON vectorClock into Hashmap
	 * @param jsonObj- JSON representation of vector clock
	 * @return vectorClockMap- Hashmap of vector clock
	 * @throws JSONException
	 */
	private HashMap<String,Integer> convertVectorClockFromJSON(JSONObject vectClock) throws JSONException{
		HashMap<String,Integer> vectorClockMap = new HashMap<String, Integer>();
		Iterator<String> keys = vectClock.keys();
		//convert each mapping to Hashmap representation
		while (keys.hasNext()) {
			String vectorKey = keys.next();
			String val = null;
			val = vectClock.getString(vectorKey);
			if (val != null) {
				vectorClockMap.put(vectorKey, Integer.parseInt(val));
			}
		}
		return vectorClockMap;
	}

	/**
	 * handlePUTReq- executes PUT request and sends updated value back to client
	 * @param jsonObj- key data sent by client
	 * @throws JSONException
	 */
	private void handlePUTReq(JSONObject jsonObj) throws JSONException {
		//Extract data and construct key value store object
		String key = jsonObj.getString("KEY");
		String value = jsonObj.getString("VALUE");
		System.out.println(jsonObj.getString("TIMESTAMP"));
		Timestamp time= Timestamp.valueOf(jsonObj.getString("TIMESTAMP"));
		JSONObject vectorClockJSON = jsonObj.getJSONObject("VECTOR_CLOCK");
		HashMap<String,Integer> vectClock=convertVectorClockFromJSON(vectorClockJSON);
		KeyValueStore newData= new KeyValueStore(key,value,time,vectClock);
		//Get write lock and check if key exists in large map
		keyLockMapLock.writeLock().lock();
		if(!keyLockMap.containsKey(key)){
			keyLockMap.put(key,new ReentrantReadWriteLock());
		}
		keyLockMapLock.writeLock().unlock();
		keyLockMap.get(key).writeLock().lock();
		try {
            //InetAddress ip = InetAddress.getLocalHost();
			//update persistent storage and note that it isn't anti-entropy process
			//String ip=getPublicIp();
            newData.updatePersistantStore();
            System.out.println(newData.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
		keyLockMap.get(key).writeLock().unlock();
		//send updated data back to client
		sendKeyValueStoreObject(newData);	
	}
	/**
	 * sendKeyValueStoreObject- sends keyValueStore object as json to client
	 * @param a KeyValueStore object that will be sent  to client
	 */
	private void sendKeyValueStoreObject(KeyValueStore a){
		JSONObject jsonObj= new JSONObject();
		try {
			jsonObj.put("KEY", a.getHexEncodedKey());
			jsonObj.put("VALUE",a.getValue());
			jsonObj.put("TIMESTAMP", a.getTimeStamp().toString());
			jsonObj.put("VECTOR_CLOCK", a.getVectorClock());
			sendJSON(jsonObj);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * sendJSON- sends a JSON object to a client through a socket.
	 * @param jsonObj- JSON object to be sent
	 */
	private void sendJSON(JSONObject jsonObj){
		try{
			OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream(),
					 StandardCharsets.UTF_8);
			out.write(jsonObj.toString());
			out.write("\n");
			out.flush();
			socket.close();
			System.out.println(jsonObj.toString());
		}
		catch(Exception e){
			e.printStackTrace();
		} 
	 }
	

	/**
	 * Function to handle get Request
	 * @param jsonObj- data sent by client
	 * @throws JSONException
	 */
	private void handleGETRequest(JSONObject jsonObj) throws JSONException {
		System.out.println("IN GETTTTT");
		String key = jsonObj.getString("KEY");
		//lock read lock
		keyLockMapLock.readLock().lock();
		//if it doesnt contain key
		if(!keyLockMap.containsKey(key)){
			//if it doesnt have the key, no value stored
			keyLockMapLock.readLock().unlock();
			JSONObject returnVals= new JSONObject();
			returnVals.put("METHOD", "NOVAL");
			//send NOVAL BACK TO CLIENT AND END CONNECTION
			sendJSON(returnVals);
			return;
		}
		else{
			keyLockMapLock.readLock().unlock();
			//get read lock of specific key
			//NOTE: IS IT ISSUE IF WRITE OCCURS TO LARGER DATA STRUCTURE???? (dont think so)
			keyLockMap.get(key).readLock().lock();
			//now can get value (construct path to serialized file)
			String fullPath=resPath+key+".ser";
			boolean releasedLock=false;
			try {
				//System.out.println(fullPath);
				//read in object from persistent storage
				FileInputStream fileIn = new FileInputStream(fullPath);
				ObjectInputStream in = new ObjectInputStream(fileIn);
				KeyValueStore cmp= (KeyValueStore) in.readObject();
				fileIn.close();
				in.close();
				keyLockMap.get(key).readLock().unlock();
				releasedLock=true;
				//create json object and return it to client
				sendKeyValueStoreObject(cmp);
				String value=cmp.getValue();
				System.out.println(value);
				System.out.flush();
			} catch (FileNotFoundException noFile) {
				//Shouldnt be case if a key is in the hashmap
				System.out.print("ERROR: WHY IS THERE KEY IN HASHMAP IF FILENOTFOUND");
				if(!releasedLock)
					keyLockMap.get(key).readLock().unlock();
			}
			catch(Exception e){
				//otherwise we have a bad exception and need to fail
				e.printStackTrace();
				if(!releasedLock)
					keyLockMap.get(key).readLock().unlock();
			}	
		}			
	}
	
	/**
	 * setInitKeyLockHashmap- set the init map on bootup. Called by KeyValueServer (main fxn)
	 * @param keyLockMap- initial keyLockMap configured at bootup
	 */
	public static void setInitKeyLockHashmap(HashMap <String,ReadWriteLock> keyLockMap){
		ClientListener.keyLockMap=keyLockMap;
	}

	/**
	 * setReplicaTracker- configure replica data (done at bootup). Called by KeyValueServer.
	 * @param replicaTracker- data about replica key ranges and IPs
	 */
	public static void setReplicaTracker(ConfigFileEntry[] replicaTracker) {
		ClientListener.replicaTracker = replicaTracker;
	}
}
