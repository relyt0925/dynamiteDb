package dynamiteDb;
import java.security.MessageDigest;

import java.security.NoSuchAlgorithmException;
import java.io.Serializable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.HashMap;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.*;
//import java.
public class KeyValueStore implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2711225303420025142L;
	/**
	 * idToClockMap- vector clock structure. Uses IP address to fetch corresponding clock value
	 */
	private HashMap<String,Integer> idToClockMap;
	/**
	 * key- key that is associated with the value. Key identifies the object being stored.
	 */
	private byte[] key;
	/**
	 * value- value of the object being stored with the key. Just a large string (can handle json)
	 */
	private String value;
	/**
	 * timeStamp-timeStamp of when data created.
	 */
	private Timestamp timeStamp;

	/**
	 * hasher- structure to do hashing of keys if needed
	 */
	private transient MessageDigest hasher;
	/**
	 * resource path- directory where to store the serialized data
	 */
	private final String resourcePath="src/main/resources/keys/";
	
	public KeyValueStore(byte[] key,String value,Timestamp timeStamp){
		idToClockMap= new HashMap<String,Integer>();
		this.value=value;
		this.timeStamp= timeStamp;
		this.key=key;
		try{
			hasher= MessageDigest.getInstance("SHA-256");
		}
		catch(Exception e){
			e.printStackTrace();
		}	
	}
	
	public KeyValueStore(String hexEncodedKey,String value, Timestamp timeStamp,HashMap<String,Integer> vectClock){
		idToClockMap=vectClock;
		this.timeStamp=timeStamp;
		this.value=value;
		try {
			this.key=Hex.decodeHex(hexEncodedKey.toCharArray());
			hasher= MessageDigest.getInstance("SHA-256");
		} catch (DecoderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch ( NoSuchAlgorithmException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Constructor- constructs Key Value Store Class from hash and a value
	 * @param key- hash (SHA-256) of the object being used
	 * @param value- String representation of the value of the object
	 */
	public KeyValueStore(byte[] key,String value){
		idToClockMap= new HashMap<String,Integer>();
		this.value=value;
		//timeStamp= new Timestamp(System.currentTimeMillis());
		this.key=key;
		try{
			hasher= MessageDigest.getInstance("SHA-256");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	/**
	 * Constructor- constructs Key Value Store Class from hash and a value
	 * @param keyName- String name of key that will be hashed
	 * @param value- String representation of the value of the object
	 */
	public KeyValueStore(String keyName,String value){
		try{
			hasher= MessageDigest.getInstance("SHA-256");
			this.key=hasher.digest(keyName.getBytes());
		}
		catch(Exception e){
			System.out.print(e);
		}
		this.value=value;
		//timeStamp= new Timestamp(System.currentTimeMillis());
		idToClockMap= new HashMap<String,Integer>();
	}	
	
	/**
	 * isEqual- compares two KeyValueStore instances
	 * @param e- KeyValueStore to compare to
	 * @return true if classes have same data
	 */
	public boolean isEqual(KeyValueStore e){
		if(!e.idToClockMap.equals(idToClockMap)){
			System.out.println("MAPS DONT MATCH");
			return false;
		}
		if(!MessageDigest.isEqual(key, e.key)){
			System.out.println("KEYS DONT MATCH");
			System.out.println(e.key.toString()+" "+key.toString());
			return false;
		}
		if(!value.equals(e.value)){
			System.out.println("Values DONT MATCH");
			System.out.println(e.value.toString()+" "+value.toString());
			return false;
		}
		if(!timeStamp.equals(e.timeStamp)){
			System.out.println("Timestamps DONT MATCH");
			return false;
		}
		if(!hasher.getAlgorithm().equals(e.hasher.getAlgorithm())){
			System.out.println("ALGORITHMS dont match");
			return false;
		}
		return true;
	}
	
	/**
	 * setHashMap- TESTING FUNCTION WILL NOT BE NEEDED IN ACTUAL IMPLEMENTATION.
	 * 		       Allows setting of different vector clocks to test possibilities of 
	 * 			   vector clock combinations from inputs.
	 * @param a-vector clock one wants to set
	 */
	public void setHashMap(HashMap<String,Integer> a){
		idToClockMap= a;
		//timeStamp=new Timestamp((long)5);
	}
	
	/**
	 * updateVectorClock- updates proper vector clock entry on serialization
	 * @param addr- IP address of the host doing serialization
	 */
	private void updateVectorClock(String addr){
		if(idToClockMap.containsKey(addr)){
			idToClockMap.put(addr, Integer.valueOf(idToClockMap.get(addr).intValue()+1));
		}
		else{
			idToClockMap.put(addr,1);
		}
		//timeStamp= new Timestamp(System.currentTimeMillis());
	}
	
	/**
	 * consolidateKeys- updates this instance with the most up to date data between
	 * 					the new store and the serialized data
	 * @param cmp- instance of class that was deserialized from persistent storage.
	 * @return true if it needs to be serialized bc new data false otherwise
	 */
	private boolean consolidateKeys(KeyValueStore cmp,String addr){
		int numGreaterPositions=0;
		int numEqualPositions=0;
		for( String key : idToClockMap.keySet()){
			if(cmp.idToClockMap.containsKey(key)){
				//then compare
				if(idToClockMap.get(key).intValue()>cmp.idToClockMap.get(key).intValue()){
					numGreaterPositions++;
				}
				else if(idToClockMap.get(key).intValue()==cmp.idToClockMap.get(key).intValue()){
					numEqualPositions++;
				}
				
			}
			else{
				//know this instance has the greater value since it is not initialized
				numGreaterPositions++;
			}
		}
		int maxSize=0;
		if(idToClockMap.size()>=cmp.idToClockMap.size())
			maxSize=idToClockMap.size();
		else
			maxSize=cmp.idToClockMap.size();
		//if not every clock greater than or equal to cmp, but at least one is greater than cmp
		if(numGreaterPositions>0 && (numGreaterPositions+numEqualPositions!=maxSize)){
			//conflict, need to merge keys on timestamp
			//ON CONFLICT, LOOK AT TIMESTAMPS FROM MASTER TO WRITE MOST RECENT VALUE
			if(cmp.timeStamp.after(timeStamp)){
				value=cmp.value;
				timeStamp=cmp.timeStamp;
			}
			//otherwise, last write (new data on network wins)
			//get max of every value
			for( String key : cmp.idToClockMap.keySet()){
				if(idToClockMap.containsKey(key)){
					//make value max of both maps
					if(idToClockMap.get(key).intValue()<cmp.idToClockMap.get(key).intValue())
						idToClockMap.put(key,cmp.idToClockMap.get(key));
				}
				else{
					//add key to the map
					idToClockMap.put(key,cmp.idToClockMap.get(key));
				}
			}	
		}
		else if(numGreaterPositions==0){
			if(numEqualPositions!=maxSize){
				//serialized data has newest entries AND no need to serialize again
				idToClockMap=cmp.idToClockMap;
				value=cmp.value;
				timeStamp=cmp.timeStamp;
				return false;
			}
			else; //had all equal vector clock values, so need to serialize data
			//this solves case of (R=1)
		}
		else;//this instance had all greater or equal vector clocks so needs to serialze
		return true;
	}
	
	/**
	 * updatePersistantStorage- compares instance in persistance storage and serializes the most
	 * 							up to date values.
	 * @param addr- IP address of host doing serialization
	 */
	public void updatePersistantStore(String addr,boolean isAntiEntropy){
		try {
			//System.out.println(resourcePath+Hex.encodeHexString(key).toString()+".ser");
			FileInputStream fileIn = new FileInputStream(resourcePath+Hex.encodeHexString(key).toString()+".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			KeyValueStore cmp= (KeyValueStore) in.readObject();
			fileIn.close();
			in.close();
			boolean needToSerialize=consolidateKeys(cmp,addr);
			//if serialized value up to date no need to serialize it
			if(!needToSerialize)
				return;
		} catch (FileNotFoundException noFile) {
			//theres no existing persistant storage, we can serialize the new entry
			System.out.print("FIRST WRITE");
		}
		catch(Exception e){
			//otherwise we have a bad exception and need to fail
			e.printStackTrace();
		}
		//update vector clock before writing IF NOT ANTI ENTROPY, OTHERWISE JUST CONSOLIDATE
		if(!isAntiEntropy)
			updateVectorClock(addr);
		try{
			FileOutputStream outFile=new FileOutputStream(resourcePath+Hex.encodeHexString(key).toString()+".ser");
			ObjectOutputStream out = new ObjectOutputStream(outFile);
			out.writeObject(this);
			out.close();
			outFile.close();	
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	@Override
	public String toString(){
		String returnString=idToClockMap.toString()+"\n";
		returnString+=idToClockMap.toString()+"\n";
		returnString+="VAL: "+value+"\n";
		returnString+=timeStamp.toString();
		return returnString;
	}
	
	/**
	 * readObject- instantiates transient data before the object is returned to the user
	 * @param in- input stream object being serialized to
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        try {
			hasher= MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
	public String getHexEncodedKey(){
		return Hex.encodeHexString(key).toString();
	}
	
	public HashMap<String,Integer> getVectorClock(){
		return idToClockMap;
	}
	
	public String getValue(){
		return value;
	}
	
	public Timestamp getTimeStamp(){
		return timeStamp;
	}
}
