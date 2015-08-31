package edu.upenn.cis455.mapreduce.master;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.upenn.cis455.mapreduce.master.WorkerStatus.Status;

public class MasterServlet extends HttpServlet {

	static final long serialVersionUID = 455555001;
	HashMap<String, WorkerStatus> workerMap;
	boolean allWaiting;
	HTTPClient client;

	// variables to store form data
	String className;
	String inputDirectory;
	String outputDirectory;
	int numMap;
	int numReduce;
	int numWorkers;

	public void init() {
		allWaiting = false;
		workerMap = new HashMap<String, WorkerStatus>();
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		PrintWriter out = response.getWriter();

		if (request.getServletPath().equals("/status")) {
			response.setContentType("text/html");
			// out.println("<html><head><title>Master</title></head>");
			// out.println("<body>Hi, I am the master!</body></html>");
			String html = "<html><head><title>Master</title></head>" + "<body>"
					+ "<table border=\"1\">" + "<tr>" + "<th>Port</th>"
					+ "<th>Status</th>" + "<th>Job</th>" + "<th>Keys Read</th>"
					+ "<th>Keys Written</th>" + "</tr>";
			synchronized (workerMap) {
				for (Entry<String, WorkerStatus> w : workerMap.entrySet()) {
					WorkerStatus ws = w.getValue();
					// display the worker status only if it's updated within the
					// last 30 seconds
					if ((new Date().getTime() - ws.getLastUpdatedStatusTime()
							.getTime()) <= 30000) {
						String jobName = null;
						if(ws.getJob() != null)
							jobName = ws.getJob().getName();
						html += "<tr>" + "<td>" + ws.getPort() + "</td>" + "<td>"
								+ ws.getStatus() + "</td>" + "<td>"
								+ jobName + "</td>" + "<td>"
								+ ws.getKeysRead() + "</td>" + "<td>"
								+ ws.getKeysWritten() + "</td>" + "</tr>";
					} else {
						workerMap.remove(w.getKey());
						System.out.println(ws.getPort()+ " died");
					}
				}
			}
			
			html += "</table>";
			html += "<br/><br/><br/>";
			String form = "<h2>Submit Jobs</h2><form action=\"\" method=\"POST\"> "
					+ "Class Name of the job:<br> "
					+ "<input type=\"text\" name=\"classname\"> "
					+ "<br>"
					+ "Input Directory:<br> "
					+ "<input type=\"text\" name=\"inputdirectory\"><br> "
					+ "Output Directory:<br> "
					+ "<input type=\"text\" name=\"outputdirectory\"><br> "
					+ "Number of Map Threads:<br> "
					+ "<input type=\"text\" name=\"nummapthreads\"><br> "
					+ "Number of Reduce Threads:<br> "
					+ "<input type=\"text\" name=\"numreducethreads\"><br> "
					+ "<input type=\"submit\" value=\"Submit Job\"> "
					+ "</form> ";
			// display the form only when there are no jobs running. can't run
			// multiple jobs at the same time
			if (allIdle()) {
				System.out.println("All idle");
				html += form;
			}
			html += "</body> " + "</html>";
			out.println(html);
			
		} else if (request.getServletPath().equals("/workerstatus")) {

			WorkerStatus ws = new WorkerStatus();

			// fetch the parameters
			int port = Integer.parseInt(request.getParameter("port"));
			System.out.println("[DEBUG] Workerstatus request from "+port);
			Status status;
			try {
				status = getStatus(request.getParameter("status"));
			} catch (Exception e1) {
				out.println("<html><body>Invalid Status. Please try again <a href=\"/status\">here</a></body></html>");
				System.out.println(e1);
				return;
			}
			Class cl = null;
			try {
				cl = Class.forName(request.getParameter("job"));
			} catch (ClassNotFoundException e) {
				out.println("<html><body>Invalid class name. Please try again <a href=\"/status\">here</a></body></html>");
				System.out.println(e);
			}
			int keysRead = Integer.parseInt(request.getParameter("keysRead"));
			int keysWritten = Integer.parseInt(request
					.getParameter("keysWritten"));
			// is client behind something?
			String ipAddress = request.getRemoteAddr();

			ws.setPort(port);
			ws.setStatus(status);
			ws.setJob(cl);
			ws.setKeysRead(keysRead);
			ws.setKeysWritten(keysWritten);
			ws.setLastUpdatedStatusTime(new Date());

			// add the status info to hash map
			synchronized (workerMap) {
				workerMap.put(ipAddress + ":" + Integer.toString(port), ws);
			}			

			if (allWaiting()) {
				System.out.println("checking if all are waiting");
				synchronized (workerMap) {
					System.out.println(workerMap);
					for (Entry<String, WorkerStatus> w : workerMap.entrySet()) {
						WorkerStatus temp = w.getValue();
						//System.out.println("Job: " +temp.getJob());
						//if(temp.getJob().getName().equals(request.getParameter("job"))) 
						{
							System.out.println("[DEBUG] Map phase done. Starting reduce..");
							try {
								client = new HTTPClient("http://" + w.getKey()
										+ "/worker/runreduce",
										"POST");
								System.out.println("-------------------------------");
								System.out.println("output Directory: "+outputDirectory);
								client.setParameter("job", request.getParameter("job"));
								client.setParameter("output", outputDirectory);
								client.setParameter("numThreads", Integer.toString(numReduce));
								client.sendRequest();
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
				
			}

		}
	}

	/**
	 * Check if all the workers for that particular job are in waiting state
	 * 
	 * @param clName
	 *            Class Name
	 * @return
	 */
	public boolean allWaiting() {
		// to do: check for active clients only
		int activeWaiting = 0;
		synchronized (workerMap) {
			for (Entry<String, WorkerStatus> w : workerMap.entrySet()) {
				WorkerStatus ws = w.getValue();
				if ((new Date().getTime() - ws.getLastUpdatedStatusTime().getTime()) <= 30000) {
					if (ws.getStatus() != Status.WAITING)
						return false;
					else
						activeWaiting += 1;
				} else {
					workerMap.remove(w.getKey());
				}
			}
		}
		
		if (activeWaiting > 0)
			return true;
		else
			return false;
	}

	/**
	 * Method indicating if all workers are busy serving a job
	 * 
	 * @return true if all workers are serving a job. false if any of them are
	 *         free
	 */
	private boolean allIdle() {
		// to do: check for active clients only
		int activeIdle = 0;
		synchronized (workerMap) {
			for (Entry<String, WorkerStatus> w : workerMap.entrySet()) {
				WorkerStatus ws = w.getValue();
				if ((new Date().getTime() - ws.getLastUpdatedStatusTime().getTime()) <= 30000) {
					if (ws.getStatus() != Status.IDLE)
						return false;
					else
						activeIdle += 1;
				} else {
					workerMap.remove(w.getKey());
				}
			}
		}		
		if (activeIdle > 0)
			return true;
		else
			return false;
	}

	/**
	 * Send map requests to all workers with the given parameters
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		// store the form data
		className = request.getParameter("classname");
		inputDirectory = request.getParameter("inputdirectory");
		outputDirectory = request.getParameter("outputdirectory");
		numMap = Integer.parseInt(request.getParameter("nummapthreads"));
		numReduce = Integer.parseInt(request.getParameter("numreducethreads"));
		
		int i = 0;
		String [] ipPort = new String[workerMap.size()];
		synchronized (workerMap) {
			for (Entry<String, WorkerStatus> w2 : workerMap.entrySet()) { 
				ipPort[i] = w2.getKey();
				i++;
			}	
		}
		
		
		for (Entry<String, WorkerStatus> w : workerMap.entrySet()) {
			WorkerStatus ws = w.getValue();
			try {
				client = new HTTPClient("http://" + w.getKey()
						 + "/worker/runmap", "POST");
				client.setParameter("job", className);
				client.setParameter("input", inputDirectory);
				client.setParameter("numThreads", Integer.toString(numMap));
				client.setParameter("numWorkers", Integer.toString(workerMap.size()));
				
				for(int j = 0; j < ipPort.length; j++) {
					client.setParameter("worker"+(j+1), ipPort[j]);
				}							
			} catch (Exception e) {
				System.out.println("Error connecting to worker "+w.getKey());
				e.printStackTrace();
			}
			
			client.sendRequest();
		}

	}

	/**
	 * Parse the string to enum Status type to store in the WorkerStatus object
	 * 
	 * @param statusString
	 * @return
	 * @throws Exception
	 */
	public Status getStatus(String statusString) throws Exception {
		switch (statusString.toUpperCase()) {
		case "MAPPING":
			return Status.MAPPING;

		case "WAITING":
			return Status.WAITING;

		case "REDUCING":
			return Status.REDUCING;

		case "IDLE":
			return Status.IDLE;

		default:
			throw new Exception("Invalid Status");
		}

	}
}
