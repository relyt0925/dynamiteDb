package dynamiteDb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * AntiEntropyDeamon Class Implements the Anti-Entropy Gossip Protocol that 
 * ensures eventual consistency of key data across data nodes
 *
 * @author Tyler Lisowski
 * @version 1.0 05/10/2016
 */
public class AntiEntopyDeamon extends DaemonService {
	/**
	 * replicaItr- iterator to choose replicas to sync with (incremented to iterate)
	 */
	private static int replicaItr=0; 
	/**
	 * Path to where key data files are stored
	 */
	private final String resourcePath="src/main/resources/keys/";
	/**
	 * lock to ensure only one thread increments replicaITr at a time
	 */
	private static final ReentrantLock lock = new ReentrantLock();
	/**
	 * Constructor- sets how often anti-entropy process runs
	 * @param frequency- how often to run the sync process
	 */
	public AntiEntopyDeamon(long frequency) {
		this.frequency = frequency;		
	}
	
	/**
	 * convertVectorClockFromJSON- gets HashMap version of vector clock from json object
	 * @param vectClock- vector clock from request in JSON format
	 * @return vector clock in hashmap format
	 * @throws JSONException- thrown when improper vector clock in request
	 */
	private HashMap<String,Integer> convertVectorClockFromJSON(JSONObject vectClock) throws JSONException{
		HashMap<String,Integer> vectorClockMap = new HashMap<String, Integer>();
		//get all IPs in the vector clock (IPs are the keys)
		Iterator<String> keys = vectClock.keys();
		//Add all keys to the hashmap (dont add if value is null)
		while (keys.hasNext()) {
			String vectorKey = keys.next();
			String val = null;
			//get value of clock for specific IP
			val = vectClock.getString(vectorKey);
			if (val != null) {
				vectorClockMap.put(vectorKey, Integer.parseInt(val));
			}
		}
		return vectorClockMap;
	}
	
	/**
	 * sendKeyValueStoreObject- sends KeyValueStore object over a socket 
	 * for Anti-Entropy process in JSON format
	 * @param a- key value store object to be sent
	 * @param socket- socket that object will be sent over
	 */
	private void sendKeyValueStoreObject(KeyValueStore a, Socket socket){
		JSONObject jsonObj= new JSONObject();
		try {
			jsonObj.put("METHOD", "ANTI_ENTROPY");
			jsonObj.put("KEY", a.getHexEncodedKey());
			jsonObj.put("VALUE",a.getValue());
			jsonObj.put("TIMESTAMP", a.getTimeStamp().toString());
			jsonObj.put("VECTOR_CLOCK", a.getVectorClock());
			sendJSON(jsonObj,socket);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * sendJSON- sends JSON object representing Key Value Store object over the socket
	 * @param jsonObj- key value store object in JSON representation
	 * @param socket- socket object should be sent to
	 */
	private void sendJSON(JSONObject jsonObj, Socket socket){
		try{
			//create output stream and specify how string will be formatted
			OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream(),
					 StandardCharsets.UTF_8);
			//write string
			out.write(jsonObj.toString());
			//end request by sending the \n and ensure it is sent
			out.write("\n");
			out.flush();
		}
		catch(Exception e){
			e.printStackTrace();
		} 
	 }

	@Override
	void start() {
		//choose replica to sync with and increment the iterator
		lock.lock();
		int indexValue=replicaItr;
		//replica tracker is read only!
		//need to skip over own nodes ip (no need to do anti-entropy process with self)
		//OWN NODE ALWAYS AT INDEX KeyValueServer.numReplicas (so skip over it else just increment)
		//and mod by the length of the tracker
		if(replicaItr==KeyValueServer.numReplicas-1)
			replicaItr=(replicaItr+2)%(ClientListener.replicaTracker.length);
		else
			replicaItr=(replicaItr+1)%(ClientListener.replicaTracker.length);
		lock.unlock();
		//Get IP to do anti-entropy process with and find key range that will be exchanged
		//Only nodes primary keys exchanged in anti-entropy process
		String ipToConnectTo= ClientListener.replicaTracker[indexValue].ipAddress;
		//port number that listening port is on
		int portNumber=13000;
		//calculate shared key range between the two nodes
		//NOTE: SET FOR 5 NODES (NEED MORE ROBUST TO HANDLE VARIABLE NODES)
		int logicalNodeDistanceAway=indexValue-KeyValueServer.numReplicas;
		int startKeyIndex;
		int endKeyIndex;
		//if syncing with a successor
		if(logicalNodeDistanceAway>0){
			//start key is based on index difference away from replica
			//ie( if only one index away start key should be the index above current node)
			//if two away startKeyIndex should just be current node
			startKeyIndex=KeyValueServer.numReplicas-(KeyValueServer.numReplicas+1-logicalNodeDistanceAway);
			endKeyIndex=KeyValueServer.numReplicas;
		}
		//if syncing with a predecessor
		else{
			startKeyIndex=ClientListener.replicaTracker.length-1;
			endKeyIndex=KeyValueServer.numReplicas+logicalNodeDistanceAway;
		}
		String startingKey=ClientListener.replicaTracker[startKeyIndex].hexEncodedKeyValue;
		String endingKey= ClientListener.replicaTracker[endKeyIndex].hexEncodedKeyValue;
		//retrieve total keyset (locks ensure no writers to key tracker when reading)
		ClientListener.keyLockMapLock.readLock().lock();
		Set<String> keys=ClientListener.keyLockMap.keySet();
		ClientListener.keyLockMapLock.readLock().unlock();
		//for each key, see if it needs to be exchanged 
		for(String i: keys){
			//System.out.println("KEY IS: "+i);
			boolean releasedReadLock=true;
			boolean releasedWriteLock=true;
			//full path to file (.ser is externsion)
			String fullPath=resourcePath+i+".ser";
			boolean isInRange=false;
			//if starting key is less than the ending key
			if(startingKey.compareTo(endingKey)<0){
				if(i.compareTo(startingKey)>0 && i.compareTo(endingKey)<=0)
					isInRange=true;
			}
			//if starting key is equal to end key exchange whole range of values
			else if(startingKey.compareTo(endingKey)==0){
				//they are same and need the whole range of values
				isInRange=true;
			}
			else{
				//if the startingKey is greater than the ending key, check both ranges
				if(i.compareTo(endingKey)<=0 || i.compareTo(startingKey)>0)
					isInRange=true;
			}
			//if its in the range, exchange key data to get up to date version
			if(isInRange){
				try {
					//read in key value store object from persistent storage
					ClientListener.keyLockMap.get(i).readLock().lock();
					releasedReadLock=false;
					FileInputStream fileIn = new FileInputStream(fullPath);
					ObjectInputStream inStrem = new ObjectInputStream(fileIn);
					KeyValueStore cmp= (KeyValueStore) inStrem.readObject();
					fileIn.close();
					inStrem.close();
					ClientListener.keyLockMap.get(i).readLock().unlock();
					releasedReadLock=true;
					//create socket with remote DB node to exchange versions of key data
					Socket remoteSocket = new Socket(InetAddress.getByName(ipToConnectTo), portNumber);
					//System.out.println(remoteSocket.isConnected());
					sendKeyValueStoreObject(cmp,remoteSocket);
					//read the updated version of the remote DB node
					BufferedReader in = new BufferedReader(new InputStreamReader(
							remoteSocket.getInputStream()));
					String input = in.readLine();
					//System.out.println(input);
					JSONObject jsonObj = new JSONObject(input);
					//close the socket if its not closed
					if(!remoteSocket.isClosed())
						remoteSocket.close();
					//construct the received object
					String key = jsonObj.getString("KEY");
					String value = jsonObj.getString("VALUE");
					Timestamp time= Timestamp.valueOf(jsonObj.getString("TIMESTAMP"));
					JSONObject vectorClockJSON = jsonObj.getJSONObject("VECTOR_CLOCK");
					HashMap<String,Integer> vectClock=convertVectorClockFromJSON(vectorClockJSON);
					KeyValueStore newData= new KeyValueStore(key,value,time,vectClock);
					//now get ready to serialize it if needed (get write lock for specific key)
					ClientListener.keyLockMap.get(i).writeLock().lock();
					releasedWriteLock=false;
					newData.updatePersistantStore();
					ClientListener.keyLockMap.get(i).writeLock().unlock();
					releasedWriteLock=true;
				}
				catch (Exception e) {
					e.printStackTrace();
					if(!releasedReadLock)
						ClientListener.keyLockMap.get(i).readLock().unlock();
					if(!releasedWriteLock)
						ClientListener.keyLockMap.get(i).writeLock().unlock();
				}				
			}	
		}
	}
}

