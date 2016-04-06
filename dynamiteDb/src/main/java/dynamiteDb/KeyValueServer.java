package dynamiteDb;

import java.io.File;
import java.net.ServerSocket;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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

	/**
     * Application method to start the server. The server runs in an infinite loop
     * listening on port 9090.  When a connection is requested, it
     * spawns a new thread to do the servicing and immediately returns
     * to listening.  
     */
	public static void main(String[] args) throws Exception{
		
		HashMap<String,ReadWriteLock> initKeyToLockMap= getListOfKeyFiles();
		ClientListener.setInitKeyLockHashmap(initKeyToLockMap);
		
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
		File folder = new File("src/main/resources");
		File[] listOfFiles = folder.listFiles();
		HashMap<String,ReadWriteLock> initKeyToLockMap = new HashMap<String,ReadWriteLock>();
		for(File i : listOfFiles){
			System.out.println(i.getName().substring(0, i.getName().lastIndexOf('.')));
			String hexKey=i.getName().substring(0, i.getName().lastIndexOf('.'));
			initKeyToLockMap.put(hexKey, new ReentrantReadWriteLock());
		}
		return initKeyToLockMap;
	}
}
