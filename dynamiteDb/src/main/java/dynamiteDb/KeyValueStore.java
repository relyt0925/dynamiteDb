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
import java.io.ObjectStreamException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap; 
import org.apache.commons.codec.binary.*;
import java.net.InetAddress;
//import java.
public class KeyValueStore implements Serializable {
	private HashMap<String,Integer> idToClockMap;
	private byte[] key;
	private String value;
	private Timestamp timeStamp;
	private transient MessageDigest hasher;
	private final String resourcePath="src/main/resources/";
	
	
	public KeyValueStore(byte[] key,String value){
		idToClockMap= new HashMap<String,Integer>();
		this.value=value;
		timeStamp= new Timestamp(System.currentTimeMillis());
		this.key=key;
		try{
			hasher= MessageDigest.getInstance("SHA-256");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public KeyValueStore(String keyName,String value){
		try{
			hasher= MessageDigest.getInstance("SHA-256");
			this.key=hasher.digest(keyName.getBytes());
		}
		catch(Exception e){
			System.out.print(e);
		}
		this.value=value;
		timeStamp= new Timestamp(System.currentTimeMillis());
		idToClockMap= new HashMap<String,Integer>();
	}
	
	public boolean isEqual(KeyValueStore e){
		if(!e.idToClockMap.equals(idToClockMap)){
			System.out.print("MAPS DONT MATCH");
			return false;
		}
		if(!MessageDigest.isEqual(key, e.key)){
			System.out.print("KEYS DONT MATCH");
			System.out.println();
			System.out.print(e.key.toString()+" "+key.toString());
			return false;
		}
		if(!value.equals(e.value)){
			System.out.print("Values DONT MATCH");
			System.out.println();
			System.out.print(e.value.toString()+" "+value.toString());
			return false;
		}
		if(!timeStamp.equals(e.timeStamp)){
			System.out.print("Timestamps DONT MATCH");
			return false;
		}
		System.out.print(hasher.getAlgorithm());
		return true;
	}
	
	public void setHashMap(HashMap<String,Integer> a){
		idToClockMap= a;
	}
	
	private void updateVectorClock(String addr){
		//assert(vectorClockPosition<vectorClock.length);
		System.out.println(addr);
		if(idToClockMap.containsKey(addr)){
			System.out.println(addr);
			idToClockMap.put(addr, Integer.valueOf(idToClockMap.get(addr).intValue()+1));
		}
		else{
			System.out.println(addr);
			idToClockMap.put(addr,1);
		}
		timeStamp= new Timestamp(System.currentTimeMillis());
	}
	
	private void consolidateKeys(KeyValueStore cmp){
		int numGreaterPositions=0;
		int numEqualPositions=0;
		for( String key : idToClockMap.keySet()){
			if(cmp.idToClockMap.containsKey(key)){
				//then compare
				System.out.println("MINE: "+idToClockMap.get(key).toString());
				System.out.println("PERSISTANT: "+cmp.idToClockMap.get(key).toString());
				if(idToClockMap.get(key).intValue()>cmp.idToClockMap.get(key).intValue()){
					numGreaterPositions++;
				}
				else if(idToClockMap.get(key).intValue()==cmp.idToClockMap.get(key).intValue()){
					numEqualPositions++;
				}
				
			}
			else{
				//know it has the greater value since it is not initialized
				numGreaterPositions++;
			}
		}
		int maxSize=0;
		if(idToClockMap.size()>=cmp.idToClockMap.size())
			maxSize=idToClockMap.size();
		else
			maxSize=cmp.idToClockMap.size();
		System.out.println(maxSize);
		if(numGreaterPositions>0 && (numGreaterPositions+numEqualPositions!=maxSize)){
			//conflict, need to merge keys on timestamp
			System.out.println("MERGING");
			if(cmp.timeStamp.after(timeStamp)){
				//idToClockMap=cmp.idToClockMap;
				value=cmp.value;
			}
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
			//take all of compares values, bc its greater
			System.out.println("TAKING CMPS STUFF");
			idToClockMap=cmp.idToClockMap;
			value=cmp.value;
		}
		else; //already have up to date values so don't need to adjust anything
	}
	
	public void updatePersistantStore(String addr){
		//UPDATE VECTOR CLOCK ON NODE AND STORE
		//write object
		//assert(MessageDigest.isEqual(cmp.key, key));
		try {
			System.out.println();
			System.out.print(resourcePath+Hex.encodeHexString(key).toString()+".ser");
			System.out.println();
			FileInputStream fileIn = new FileInputStream(resourcePath+Hex.encodeHexString(key).toString()+".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			KeyValueStore cmp= (KeyValueStore) in.readObject();
			consolidateKeys(cmp);
			fileIn.close();
			in.close();
		} catch (FileNotFoundException noFile) {
			//theres no existing persistant storage
			System.out.print("FIRST WRITE");
		}
		catch(Exception e){
			e.printStackTrace();
		}
		//update vector clock before writing
		updateVectorClock(addr);
		try{
			System.out.println();
			System.out.println();
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
	
	public String toString(){
		String returnString=idToClockMap.toString()+"\n";
		returnString+=idToClockMap.toString()+"\n";
		returnString+="VAL: "+value+"\n";
		returnString+=timeStamp.toString();
		return returnString;
	}
	
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        try {
			hasher= MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
