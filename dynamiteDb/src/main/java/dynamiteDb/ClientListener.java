package dynamiteDb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.codec.binary.Hex;
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
	private Socket socket;
	private String key;
	private String method;
	private String value;
	private HashMap<String,String> vectorMap;
	private static HashMap <String,ReadWriteLock> keyLockMap;
	private static ReadWriteLock keyLockMapLock= new ReentrantReadWriteLock();
	private final String resPath="src/main/resources/";


	public ClientListener(Socket socket) {
		this.socket = socket;
	}

	/**
	 * Services this thread's client request.
	 */
	public void run() {
		try {

			// Read the byte array from the socket
			BufferedReader in = new BufferedReader(new InputStreamReader(
					socket.getInputStream()));
			// Write to the client
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

			// Get messages from the client
			String input = in.readLine();
			JSONObject jsonObj = new JSONObject(input);
			
			//Based upon the method perform call appropriate handling function
			method = jsonObj.getString("METHOD");
			
			//Based upon the method call appropriate handler function
			if(KeyValueServer.METHOD_GET.equals(method)){
				handleGETRequest(jsonObj);
			}
			if(KeyValueServer.METHOD_PUT.equals(method)){
				handlePUTReq(jsonObj);
			}
			if(KeyValueServer.METHOD_ANTI_ENTROPY.equals(method)){
				handleANTI_ENTROPYReq(jsonObj);
			}
			
			//Debug Purpose only
			System.out.println("KEY = "+key + "; METHOD = "+method+"; VALUE = "+value);
			System.out.println(Arrays.asList(vectorMap)); 
			
			

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
	 * Function to handle anti entropy request 
	 * @param jsonObj
	 * @throws JSONException
	 */
	private void handleANTI_ENTROPYReq(JSONObject jsonObj) throws JSONException {

		JSONObject vectorClockJObj = jsonObj.getJSONObject("VECTOR_CLOCK");
		vectorMap = new HashMap<String, String>();
		Iterator<String> keys = vectorClockJObj.keys();
		while (keys.hasNext()) {
			String vectorKey = keys.next();
			String val = null;
			val = vectorClockJObj.getString(vectorKey);
			if (val != null) {
				vectorMap.put(vectorKey, val);
			}
		}		
	}
	
	
	
	/**
	 * Function to handle put request
	 * @param jsonObj
	 * @throws JSONException
	 */

	private void handlePUTReq(JSONObject jsonObj) throws JSONException {

		key = jsonObj.getString("KEY");
		value = jsonObj.getString("VALUE");
		
		
	}
	
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
	
	private void sendJSON(JSONObject jsonObj){
		try{
			OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream(),
					 StandardCharsets.UTF_8);
			out.write(jsonObj.toString());			                     
		}
		catch(Exception e){
			e.printStackTrace();
		} 
	 }
	

	/**
	 * Function to handle get Request
	 * @param jsonObj
	 * @throws JSONException
	 */
	private void handleGETRequest(JSONObject jsonObj) throws JSONException {
		key = jsonObj.getString("KEY");
		//lock read lock
		keyLockMapLock.readLock().lock();
		//see if key exsits
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
			//NOTE: IS IT ISSUE IF WRITE OCCURS TO LARGER DATA STRUCTURE????
			keyLockMap.get(key).readLock().lock();
			//now can get value
			//key+=".ser";
			String fullPath=resPath+key+".ser";
			try {
				//System.out.println(fullPath);
				FileInputStream fileIn = new FileInputStream(fullPath);
				ObjectInputStream in = new ObjectInputStream(fileIn);
				KeyValueStore cmp= (KeyValueStore) in.readObject();
				fileIn.close();
				in.close();
				keyLockMap.get(key).readLock().unlock();
				//create json object and return it to client
				sendKeyValueStoreObject(cmp);
			} catch (FileNotFoundException noFile) {
				//Shouldnt be case if a key is in the hashmap
				System.out.print("ERROR: WHY IS THERE KEY IN HASHMAP IF FILENOTFOUND");
			}
			catch(Exception e){
				//otherwise we have a bad exception and need to fail
				e.printStackTrace();
			}	
		}			
	}
}
