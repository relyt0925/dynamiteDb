package dynamiteDb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

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

	/**
	 * Function to handle get Request
	 * @param jsonObj
	 * @throws JSONException
	 */
	private void handleGETRequest(JSONObject jsonObj) throws JSONException {
		
		key = jsonObj.getString("KEY");
				
	}

}
