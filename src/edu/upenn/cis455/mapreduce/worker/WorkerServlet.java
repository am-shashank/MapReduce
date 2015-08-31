package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.master.HTTPClient;
import edu.upenn.cis455.mapreduce.master.WorkerStatus;
import edu.upenn.cis455.mapreduce.master.WorkerStatus.Status;

/**
 * Worker which implements map and reduce
 * 
 * @author Shashank
 *
 */
public class WorkerServlet extends HttpServlet {

	static final long serialVersionUID = 455555002;

	// init parameters
	String masterAddress;
	String storageDirectory;

	WorkerStatus worker;
	HTTPClient client;

	Job myJob;
	int numMapThreads;
	int numWorkers;
	String inputDirectory;
	ArrayList<Thread> mapThreads;
	ArrayList<BufferedReader> readerList;

	// ith item in the list contains the writer to the ith worker
	ArrayList<BufferedWriter> spoolOutWriters;

	int readerIndex = 0;
	BigDecimal sha1Length;
	HashMap<String, String> workers;
	File spoolOut;
	File spoolIn;
	Context mapContext;

	MessageDigest md;
	String outputDirectory;
	int numReduceThreads;
	ArrayList<Thread> reduceThreads;
	BufferedReader sortedFileReader;
	Context reduceContext;

	/**
	 * Get the init parameters and send workerstatus request every 10 seconds
	 */
	public void init() {
		try {
			md = MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		workers = new HashMap<String, String>();
		mapThreads = new ArrayList<Thread>();
		spoolOutWriters = new ArrayList<BufferedWriter>();
		readerList = new ArrayList<BufferedReader>();
		worker = new WorkerStatus();
		masterAddress = getServletConfig().getInitParameter("master");
		storageDirectory = getServletConfig().getInitParameter("storagedir");
		int port = Integer
				.parseInt(getServletConfig().getInitParameter("port"));
		synchronized (worker) {
			worker.setPort(port);
		}
		
		reduceThreads = new ArrayList<Thread>();

		/*********uncomment this to test runmap standalone*****************/
		//sha1Length = new BigDecimal(Math.pow(2, 160)).divide(BigDecimal.valueOf(2), RoundingMode.FLOOR);
		//numWorkers = 2;
		
		 // initialize the worker status 
		 Thread t = new Thread() { 
			 public void run() {
				 while (true) { 
					String jobName = null;
					if(worker.getJob()!=null) {
						jobName = worker.getJob().getName();
					}
					 try { 
						 client = new HTTPClient("http://" + masterAddress + "/master/workerstatus", "GET");
						 client.setParameter("job", jobName);
						 client.setParameter("keysRead",Integer.toString(worker.getKeysRead()));
						 client.setParameter("keysWritten", Integer.toString(worker.getKeysWritten()));
						 client.setParameter("status", worker.getStatus() .toString());
						 client.setParameter("port", Integer.toString(worker.getPort()));
						 client.sendRequest(); Thread.sleep(10000); 
						 } catch(InterruptedException e) { 
							 e.printStackTrace(); 
						 } catch (Exception e1) {
							 e1.printStackTrace(); 
						}
					 }
				 }
			 }; t.start();		 
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>Worker</title></head>");
		out.println("<body>Hi, I am the worker!</body></html>");
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {

		if (request.getServletPath().equals("/runmap")) {
			System.out.println("[DEBUG] Running Map.....");
			
			synchronized (worker) {
				worker.setStatus(Status.MAPPING);

				String jobName = request.getParameter("job");
				
				inputDirectory = request.getParameter("input");
				numMapThreads = Integer
						.parseInt(request.getParameter("numThreads"));
				numWorkers = Integer.parseInt(request.getParameter("numWorkers"));

				for (int i = 1; i <= numWorkers; i++) {
					workers.put("worker" + i, request.getParameter("worker" + i));
				}
				
				Class jobClass;
				try {
					jobClass = Class.forName(jobName);
					worker.setJob(jobClass);
					System.out.println("=======================");
					System.out.println(worker.getJob().getName());
					myJob = (Job) jobClass.newInstance();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
			
			sha1Length = new BigDecimal(Math.pow(2, 160)).divide(
					BigDecimal.valueOf(numWorkers), RoundingMode.FLOOR);


			readerList = getReaderList();

			spoolOut = mkdirHelper(storageDirectory + "spool-out");
			spoolIn = mkdirHelper(storageDirectory + "spool-in");
			spoolOutWriters = getWriterList();
			mapContext = new MapWriter();

			// Instantiate multiple threads
			for (int i = 0; i < numMapThreads; i++) {
				// Thread responsible for reading the key-value pairs and
				// invokes map
				Thread t = new Thread() {
					public void run() {
						int i = 0;
						while (true) {
							String line = null;
							// read one line from a Thread in a synchronized
							// manner
							synchronized (readerList) {
								if (readerIndex == readerList.size())
									break;

								BufferedReader br = readerList.get(readerIndex);
								try {
									line = br.readLine();
									// get the next reader if you have reached
									// the end of file
									if (line == null) {
										readerIndex++;
										if (readerIndex == readerList.size())
											break;
										else
											continue;
									} else {
										synchronized (worker) {
											worker.setKeysRead(worker.getKeysRead() + 1);
										}
									}

								} catch (IOException e) {
									e.printStackTrace();
								}
							}

							if (line != null) {
								String[] keyValue = line.split(" ", 2);
								myJob.map(keyValue[0], keyValue[1], mapContext);
							}
							i++;
						}
					}
				};
				mapThreads.add(t);

			}

			for (Thread t : mapThreads)
				t.start();

			for (Thread t : mapThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			// close the writers
			close();

			// send pushdata
			try {
				sendPushData();
			} catch (Exception e) {
				e.printStackTrace();
			}


			// send workserstatus
			try {
				 client = new HTTPClient("http://" + masterAddress +"/master/workerstatus", "GET"); client.setParameter("job",
				 worker.getJob().getName()); client.setParameter("keysread",Integer.toString(worker.getKeysRead()));
				 client.setParameter("keyswritten", Integer.toString(worker.getKeysWritten()));
				 client.setParameter("status", worker.getStatus().toString()); 
				 client.setParameter("port", Integer.toString(worker.getPort()));
				 client.sendRequest();
			} catch (Exception e) {
				e.printStackTrace();
			}

			// change the status to reduce
		} else if (request.getServletPath().equals("/pushdata")) {
			System.out.println("[DEBUG] Pushing Data.....");
			BufferedReader br = request.getReader();
			if(spoolIn == null)
				spoolIn = new File(storageDirectory+"spool-in");
			File f = new File(storageDirectory + "spool-in/myworker");
			synchronized (f) {
				BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));
				String line = "";
				while ((line = br.readLine()) != null) {
					bw.write(line + "\n");
				}
				br.close();
				bw.close();
				System.out.println("[DEBUG] Successfully pushed data");
			}
		} else if (request.getServletPath().equals("/runreduce")) {
			try {
				handleRunReduce(request, response);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Handler for runreduce request
	 * 
	 * @param request
	 * @param response
	 * @throws Exception
	 */
	private void handleRunReduce(HttpServletRequest request,
			HttpServletResponse response) throws Exception {
		synchronized (worker) {
			worker.setStatus(Status.REDUCING);
		}
		
		// store the request parameters
		String job = request.getParameter("job");
		outputDirectory = request.getParameter("output");
		Class temp;
		try {
			temp = Class.forName(job);
			myJob = (Job) temp.newInstance();
			synchronized (worker) {
				worker.setJob(temp);
			}
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		File o = mkdirHelper(storageDirectory + outputDirectory);
		if(spoolIn == null)
			spoolIn = new File(storageDirectory+"spool-in");
		File pushedDataFile = spoolIn.listFiles()[0];
		if (pushedDataFile == null)
			throw new Exception("Spool in directory does not have nay files");
		int p = Runtime
				.getRuntime()
				.exec("sort -o " + pushedDataFile.getAbsolutePath()
						+ "_sorted " + pushedDataFile.getAbsolutePath())
				.waitFor();
		if(p != 0)
			throw new Exception("Error running sort command. Didn't exit with 0 status");
		numReduceThreads = Integer.parseInt(request.getParameter("numThreads"));
		File f = new File(pushedDataFile.getAbsolutePath() + "_sorted");
		sortedFileReader = new BufferedReader(new FileReader(f));
		
		// create a writer to a result file where resultant key values will be stored
		File result;
		if(outputDirectory.endsWith("/"))
			result = new File(storageDirectory+outputDirectory+"myworker_result");
		else
			result = new File(storageDirectory+outputDirectory+"/myworker_result");
		BufferedWriter bw = new BufferedWriter(new FileWriter(result));
		reduceContext = new ReduceWriter(bw);
		
		for (int i = 1; i <= numReduceThreads; i++) {
			Thread t = new Thread() {
				KeyValues kv = null;
				String prevLine = "";
				String key = "";
				String line = "";

				public void run() {
					try {
						while (true) {
							if (prevLine == null)
								break;
							synchronized (sortedFileReader) {
								// handle the initial case
								if (kv == null) {
									if (prevLine == "")
										prevLine = sortedFileReader.readLine();
									if (prevLine == null)
										break;
									kv = new KeyValues(prevLine);
									synchronized (worker) {
										worker.setKeysRead(worker.getKeysRead() + 1);
									}									
								}
								while (((line = sortedFileReader.readLine()) != null)
										&& (key = line.split("\t")[0].trim())
												.equals(kv.getKey())) {
									kv.addValue(line.split("\t")[1].trim());
									synchronized (worker) {
										worker.setKeysRead(worker.getKeysRead() + 1);
									}
								}
							}
							System.out.println("Key: " + kv.getKey()+" Values: "+kv.getValues());
							String[] values = new String[kv.getValues().size()];
							kv.getValues().toArray(values);
							myJob.reduce(kv.getKey(), values, reduceContext);

							prevLine = line;
							kv = null;
							key = "";
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			};
			reduceThreads.add(t);
		}

		for (Thread t : reduceThreads)
			t.start();

		// change status to IDLE once everything is done
		/*synchronized (worker) {
			worker.setStatus(Status.IDLE);
		}*/
		
		for(Thread t: reduceThreads)
			t.join();
			
		
		// close the writer
		bw.close();
	}

	/**
	 * Go through the list of files in the spool-out directory and issue a POST
	 * to the /pushdata
	 * 
	 * @throws Exception
	 */
	public void sendPushData() {
		File[] files = spoolOut.listFiles();
		for (File f : files) {
			if (f.isFile()) {
				String urlString = "http://" + workers.get(f.getName())
						+ "/worker/pushdata";
				try {
					client = new HTTPClient(urlString, "POST");
					client.setBody(readFile(f));
					client.sendRequest();
					
					// change the status of the worker once the data has reached the endpoint
					if(client.statusCode == 200) {
						worker.setStatus(Status.WAITING);
						System.out.println("worker in waiting state");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Read the contents of a file into a string
	 * 
	 * @param path
	 * @param encoding
	 * @return
	 * @throws IOException
	 */
	String readFile(File f) throws IOException {
		try {
			byte[] bytes = Files.readAllBytes(f.toPath());
			return new String(bytes, "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";

	}

	public void close() throws IOException {
		for (BufferedWriter br : spoolOutWriters) {
			br.close();
		}
	}

	/**
	 * Write the key value pairs to the appropriate worker file by using SHA-1
	 * 
	 * @author Shashank
	 *
	 */
	public class MapWriter implements Context {

		public MapWriter() {

		}

		@Override
		public synchronized void write(String key, String value) {
			md.update(key.getBytes());
			byte[] sha1 = md.digest();
			BigInteger temp = new BigInteger(1, sha1);
			
			BigDecimal workerId = new BigDecimal(temp).divide(sha1Length,
					RoundingMode.FLOOR);
			int index = workerId.intValue() + 1;
			
			if (index > numWorkers)
				index = numWorkers;
			
			// convert the 1 based index to 0
			int writerIndex = index - 1;
			
			// hash the workerid and get the appropriate file handle to append
			// the key value pair
			BufferedWriter br = spoolOutWriters.get(writerIndex);
			try {
				br.write(key + "\t" + value + "\n");
				worker.setKeysWritten(worker.getKeysWritten() + 1);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
	
	/**
	 * Write the key value pairs to the appropriate worker file by using SHA-1
	 * 
	 * @author Shashank
	 *
	 */
	public class ReduceWriter implements Context {

		BufferedWriter bw;
		/*public ReduceWriter(String path) throws IOException {
			File output = new File(path);
			bw = new BufferedWriter(new FileWriter(output, true));
		}*/

		public ReduceWriter(BufferedWriter bw) {
			this.bw = bw;
		}
		@Override
		public synchronized void write(String key, String value) {
			try {
				bw.write(key + "\t" + value + "\n");
				synchronized (worker) {
					worker.setKeysWritten(worker.getKeysWritten() + 1);
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * Create a list of buffered readers to all the files in the input directory
	 * 
	 * @return
	 * @throws IOException
	 */
	public ArrayList<BufferedReader> getReaderList() throws IOException {
		ArrayList<BufferedReader> temp = new ArrayList<BufferedReader>();
		File dir = new File(storageDirectory + inputDirectory);
		File[] files = dir.listFiles();

		for (File f : files) {
			if (f.isFile()) {
				BufferedReader inputStream = null;
				try {
					inputStream = new BufferedReader(new FileReader(f));
					temp.add(inputStream);
				} finally {
					/*
					 * if (inputStream != null) { inputStream.close(); }
					 */
				}
			}
		}
		return temp;
	}

	/**
	 * Creates a list of buffered writers to all the files in the specified
	 * directory
	 * 
	 * @return
	 */
	public ArrayList<BufferedWriter> getWriterList() {
		ArrayList<BufferedWriter> temp = new ArrayList<BufferedWriter>();
		for (int i = 1; i <= numWorkers; i++) {
			File file = new File(storageDirectory + "/spool-out/worker" + i);
			try {
				BufferedWriter outputStream = new BufferedWriter(
						new FileWriter(file));
				temp.add(outputStream);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return temp;
	}

	/**
	 * Create the directory with the specified path if it doesn't exist
	 * 
	 * @param path
	 * @return
	 */
	public File mkdirHelper(String path) {
		File file = new File(path);
		boolean isDirectoryCreated = file.mkdir();
		if (isDirectoryCreated) {
			System.out.println("[DEBUG] Creating " + path);

		} else {
			System.out.println("[DEBUG] Deleting " + path);
			deleteDir(file); // Invoke recursive method
			file.mkdir();
		}
		return file;
	}

	/**
	 * Delete the directory recursively and delete all files inside the
	 * directory
	 * 
	 * @param dir
	 */
	public void deleteDir(File dir) {
		File[] files = dir.listFiles();

		for (File myFile : files) {
			if (myFile.isDirectory()) {
				deleteDir(myFile);
			}
			myFile.delete();

		}
	}

}
