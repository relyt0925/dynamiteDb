package dynamiteDb;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StatsDaemon extends DaemonService {
	
	Logger log = Logger.getLogger(KeyValueServer.LOG);

	public StatsDaemon(long frequency) {
		this.frequency = frequency;		
	}

	@Override
	void start() {
		File folder = new File("src/main/resources/keys");
		File[] listOfFiles = folder.listFiles();
		long numKeys = 0;
		for(File i : listOfFiles){
			String hexKey=i.getName().substring(0, i.getName().lastIndexOf('.'));
			if(hexKey.isEmpty()){
				continue;				
			}
			numKeys++;
		}
		log.log(Level.INFO,"Current Load = "+numKeys);
	}
	
	

}
