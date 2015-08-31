/**
 * 
 */
package edu.upenn.cis455.mapreduce.master;

import java.util.Date;

/** Maintains the state information about a particular worker
 * @author Shashank
 *
 */
public class WorkerStatus {
	
	int port;
	Status status;
	Class job;
	int keysRead;
	int keysWritten;
	Date lastUpdatedStatusTime;
	
	/**
	 * Default constructor which initializes the members to default values
	 */
	public WorkerStatus() {
		status = Status.IDLE;
		job = null;
		keysRead = 0;
		keysWritten = 0;		
	}

	public Date getLastUpdatedStatusTime() {
		return lastUpdatedStatusTime;
	}

	public void setLastUpdatedStatusTime(Date lastUpdatedStatusTime) {
		this.lastUpdatedStatusTime = lastUpdatedStatusTime;
	}
	
	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	

	public Class getJob() {
		return job;
	}

	public void setJob(Class job) {
		this.job = job;
	}

	public int getKeysRead() {
		return keysRead;
	}

	public void setKeysRead(int keysRead) {
		this.keysRead = keysRead;
	}

	public int getKeysWritten() {
		return keysWritten;
	}

	public void setKeysWritten(int keysWritten) {
		this.keysWritten = keysWritten;
	}

	public enum Status {
		MAPPING, WAITING, REDUCING, IDLE
	};

}
