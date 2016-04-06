package dynamiteDb;

public abstract class DaemonService {

	long frequency = 1;
	long count = 1;
	
	public DaemonService(){
		this.frequency = 1;
	}
	/**
	 * set the deamon execution frequency
	 * @param seconds
	 */
	void setFrequency(long seconds){
		this.frequency = seconds;
	}
	
	/**
	 * Start the deamon service
	 */
	abstract void start();
	
	/**
	 * return the execution frequency
	 */
	long getFrequency(){
		return frequency;
	}
	
	
	/**
	 * set current count which tells if the deamon process can be started 
	 * @param count
	 */
	void incrementCount(){
		count++;
	}
	
	/**
	 * check if service is schedulable
	 * @return
	 */
	boolean shouldSchedule(){
		if(count==frequency){
			count = 0;
			return true;
		}
		return false;
	}
}
