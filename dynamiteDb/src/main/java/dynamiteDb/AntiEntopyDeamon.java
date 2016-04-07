package dynamiteDb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

public class AntiEntopyDeamon extends DaemonService {

	private static int replicaItr=0; 
	private final String resourcePath="src/main/resources/keys/";
	private static final ReentrantLock lock = new ReentrantLock();
	public AntiEntopyDeamon(long frequency) {
		this.frequency = frequency;		
	}
	
	private HashMap<String,Integer> convertVectorClockFromJSON(JSONObject vectClock) throws JSONException{
		HashMap<String,Integer> vectorClockMap = new HashMap<String, Integer>();
		Iterator<String> keys = vectClock.keys();
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
	
	private void sendKeyValueStoreObject(KeyValueStore a, Socket socket){
		JSONObject jsonObj= new JSONObject();
		try {
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
	
	private void sendJSON(JSONObject jsonObj, Socket socket){
		try{
			OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream(),
					 StandardCharsets.UTF_8);
			out.write(jsonObj.toString());
			out.write("\n");
			out.flush();
			out.close();
			//socket.close();
			System.out.println(jsonObj.toString());
		}
		catch(Exception e){
			e.printStackTrace();
		} 
	 }

	@Override
	void start() {
		// TODO Auto-generated method stub
		System.out.println("Anti-Entropy Process");	
		lock.lock();
		int indexValue=replicaItr+1;
		//replica tracker is read only!
		replicaItr=(replicaItr+1)%(ClientListener.replicaTracker.length-1);
		lock.unlock();
		//Get IP to do anti-entropy process with and find key range that will be exchanged
		//Only nodes primary keys exchanged in anti-entropy process
		String ipToConnectTo= ClientListener.replicaTracker[indexValue].ipAddress;
		int portNumber=13000;
		String startingKey=ClientListener.replicaTracker[0].hexEncodedKeyValue;
		String endingKey= ClientListener.replicaTracker[0].hexEncodedKeyValue;
		//retrieve total keyset 
		ClientListener.keyLockMapLock.readLock().lock();
		Set<String> keys=ClientListener.keyLockMap.keySet();
		ClientListener.keyLockMapLock.readLock().unlock();
		//for each key, see if it needs to be exchanged 
		for(String i: keys){
			ClientListener.keyLockMap.get(i).readLock().lock();
			boolean releasedReadLock=false;
			boolean releasedWriteLock=true;
			String fullPath=resourcePath+i+".ser";
			if(i.compareTo(startingKey)>=0 && i.compareTo(endingKey)<0){
				try {
					//System.out.println(fullPath);
					//read in key value store object from persistent storage
					FileInputStream fileIn = new FileInputStream(fullPath);
					ObjectInputStream inStrem = new ObjectInputStream(fileIn);
					KeyValueStore cmp= (KeyValueStore) inStrem.readObject();
					fileIn.close();
					inStrem.close();
					ClientListener.keyLockMap.get(i).readLock().unlock();
					releasedReadLock=true;
					//create socket with remote DB node to exchange versions of key data
					Socket remoteSocket = new Socket(InetAddress.getByName(ipToConnectTo), portNumber);
					sendKeyValueStoreObject(cmp,remoteSocket);
					String valueOfSentObject=cmp.getValue();
					System.out.println(valueOfSentObject);
					//read the updated version of the remote DB node
					BufferedReader in = new BufferedReader(new InputStreamReader(
							remoteSocket.getInputStream()));
					String input = in.readLine();
					JSONObject jsonObj = new JSONObject(input);
					remoteSocket.close();
					//construct the received object
					String key = jsonObj.getString("KEY");
					String value = jsonObj.getString("VALUE");
					System.out.println(jsonObj.getString("TIMESTAMP"));
					Timestamp time= Timestamp.valueOf(jsonObj.getString("TIMESTAMP"));
					JSONObject vectorClockJSON = jsonObj.getJSONObject("VECTOR_CLOCK");
					HashMap<String,Integer> vectClock=convertVectorClockFromJSON(vectorClockJSON);
					KeyValueStore newData= new KeyValueStore(key,value,time,vectClock);
					//now get ready to serialize it if needed
					ClientListener.keyLockMap.get(i).writeLock().lock();
					releasedWriteLock=false;
					//ip doesnt matter since it is anti entropy
					String ip="DOESNTMATTER";
					newData.updatePersistantStore(ip,true);
					ClientListener.keyLockMap.get(i).writeLock().unlock();
					releasedWriteLock=true;
				}
				catch (Exception e) {
					//Shouldnt be case if a key is in the hashmap
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

