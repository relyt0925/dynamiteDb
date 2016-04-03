package dynamiteDb;

import java.net.ServerSocket;

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
		
		ServerSocket listener = new ServerSocket(9898);
	        try {
	            while (true) {
	                new ClientListener(listener.accept()).start();
	            }
	        } finally {
	            listener.close();
	        }
		
	}
}
