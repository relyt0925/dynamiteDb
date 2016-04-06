package dynamiteDb;

public class AntiEntopyDeamon extends DaemonService {

	public AntiEntopyDeamon(long frequency) {
		this.frequency = frequency;
	}

	@Override
	void start() {
		// TODO Auto-generated method stub
		System.out.println("Anti-Entropy Process");
		
	}

}
