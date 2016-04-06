package dynamiteDb;

import java.util.ArrayList;
import java.util.TimerTask;

public class DaemonServicesHandler extends TimerTask{
	
	private static ArrayList<DaemonService> daemonServiceList;

	public DaemonServicesHandler() {
		if(daemonServiceList == null)
			daemonServiceList = new ArrayList<DaemonService>();
	}
	
	/**
	 * Add service to the Daemon List
	 * @param service
	 */
	public void addDaemonService(DaemonService service ) {
		daemonServiceList.add(service);
	}
	
	/**
	 * Schedule Daemon Threads like Anti-Entropy Process, Clean up
	 */
	@Override
	public void run() {
		for(DaemonService service : daemonServiceList){
			if(service.shouldSchedule()){
				DaemonTask dTask = new DaemonTask(service);
				dTask.start();
			}
			else
			{
				service.incrementCount();
			}
		}
		
		
	}
	
	
	/**
	 * Creates a daemon thread and starts executing the start method defined in the DaemonService
	 * abstract class
	 * @author Satyajeet
	 *
	 */
	private class DaemonTask extends Thread{
		
		private DaemonService service;
		
		public DaemonTask(DaemonService service){
			this.service = service;
		}
		
		public void run() {
			this.service.start();
		}
		
		
	}

}
