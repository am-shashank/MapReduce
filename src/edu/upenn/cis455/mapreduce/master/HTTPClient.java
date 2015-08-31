/**
 * 
 */
package edu.upenn.cis455.mapreduce.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.net.Socket;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import javax.net.ssl.HttpsURLConnection;

/**
 * @author cis455
 *
 */
public class HTTPClient {

	String host;
	URL url;
	PrintWriter s_out = null;
	BufferedReader s_in = null;
	String responseHeader = null;
	public int statusCode;
	String contentType = "";
	String urlString;
	String method = "GET";
	HashMap<String, String> requestHeader = new HashMap<String, String>();
	HashMap<String, String> headerMap = new HashMap<String, String>();
	HashMap<String, String> parameterMap = new HashMap<String, String>();
	HttpsURLConnection httpsCon;
	boolean requestSent = false;
	String message;
	boolean isBodyRead = false;
	boolean isBodySet;
	String body;
	
	/**
	 * Constructor initializes the Streams of the socket
	 * 
	 * @param urlString
	 * @throws Exception
	 */
	public HTTPClient(String urlString, String method) throws Exception {
		isBodySet = false;
		this.method = method;
		
		System.out.println("[DEBUG] "+method+": "+urlString);
		// use HTTP as the default protocol
		if (!urlString.startsWith("http")) {
			urlString = "http://" + urlString;
		}
		
		this.urlString = urlString;
		this.url = new URL(urlString);
		// handle HTTPS using openconnection()
		if (url.getProtocol().equalsIgnoreCase("https")) {
			httpsCon = (HttpsURLConnection) url
					.openConnection();
			httpsCon.setRequestMethod(method);
			// Disable automatic redirects
			httpsCon.setInstanceFollowRedirects(false);
			httpsCon.setRequestProperty("User-Agent:", "cis455Crawler");			
		} else {
			try {
				int port = url.getPort();
				if (port <= 0)
					port = 80;
				Socket clientSocket = new Socket(url.getHost(), port);
				// set socket timeout to avoid blocking the servlet
				clientSocket.setSoTimeout(10000);
				// writer for socket
				s_out = new PrintWriter(clientSocket.getOutputStream(), true);
				// reader for socket
				s_in = new BufferedReader(new InputStreamReader(
						clientSocket.getInputStream()));
				message = method + " " + urlString
						+ " HTTP/1.0\r\nHost: " + url.getHost()
						+ "\r\nUser-agent: cis455Crawler\r\n";
				
			} catch (IOException e) {
				System.out.println("Error creating ClientSocket to remote host "
								+ urlString);
				e.printStackTrace();
			}
		}
		//System.out.println("Connected to "+url.getHost());
	}
	
	/**
	 * Clone the given HTTPClient object
	 * @param redirectedClient
	 */
	public void clone(HTTPClient redirectedClient) {
		host = redirectedClient.host;
		url = redirectedClient.url;
		s_out = redirectedClient.s_out;
		s_in = redirectedClient.s_in;
		responseHeader = redirectedClient.responseHeader;
		statusCode = redirectedClient.statusCode;
		contentType = redirectedClient.contentType;
		urlString = redirectedClient.urlString;
		method = redirectedClient.method;
		requestHeader = redirectedClient.requestHeader;
		headerMap = redirectedClient.headerMap;
		httpsCon = redirectedClient.httpsCon;
		requestSent = redirectedClient.requestSent;
		message = redirectedClient.message;
	}
	
	/**
	 * Sends a HTTP request with the given message and also parses the response header
	 * @param message
	 * @throws Exception 
	 */
	public void sendRequest() { 
		try {
			requestSent = true;
			if (url.getProtocol().equalsIgnoreCase("https")) {
				httpsCon.connect();
				statusCode = httpsCon.getResponseCode();
				//if(statusCode == )
				contentType = httpsCon.getContentType();
				s_in = new BufferedReader(new InputStreamReader(
						httpsCon.getInputStream()));
				Map<String, List<String>> obj = httpsCon.getHeaderFields();
				//System.out.println(httpsCon.getResponseMessage());
				//System.out.println("HTTPS Response Headers");
				for(String key: obj.keySet()) {
					if(key != null) {
						//System.out.println(key+": "+obj.get(key).get(0));
						headerMap.put(key.trim().toLowerCase(), obj.get(key).get(0).trim());
					}
				}
			} else {
				String postBody = "";
				if(isBodySet == true && method.equalsIgnoreCase("POST")) {
					postBody = body;
					requestHeader.put("Content-Type", "application/json");
					requestHeader.put("Content-Length", Integer.toString(postBody.length()));
				}
				String queryString = "";
				if(method.equalsIgnoreCase("POST") && parameterMap.size() != 0) {
					// set post body
					for(Entry<String, String> entry: parameterMap.entrySet()) {
						postBody += entry.getKey() + "=" + entry.getValue() + "&";
					}	
					requestHeader.put("Content-Type", "application/x-www-form-urlencoded");
					requestHeader.put("Content-Length", Integer.toString(postBody.length()));
				} else if(method.equalsIgnoreCase("GET") && parameterMap.size() != 0) {
					// set query string
					queryString = "?";
					for(Entry<String, String> entry: parameterMap.entrySet()) {
						queryString += entry.getKey() + "=" + entry.getValue() + "&";
					}
					
					message = method + " " + urlString + queryString
							+ " HTTP/1.0\r\nHost: " + url.getHost()
							+ "\r\nUser-agent: cis455Crawler\r\n";
				}
				
				// append the header fields to the request message
				for(String key: requestHeader.keySet()) {
					message += key+": "+requestHeader.get(key) +"\r\n";
				}
				message += "\r\n";			
				if(method.equalsIgnoreCase("POST") && postBody != "") {
					message += postBody;
				}
				
				// to do: add query string
				
				
				
				s_out.println(message);
				
				
				// Get response line and check status 
				String responseLine;
				//System.out.println("-----------------------");
				//System.out.println("Response Header");
				responseLine = s_in.readLine();
				//System.out.println(responseLine);
				StringTokenizer tokens = new StringTokenizer(responseLine); 
				String httpversion = tokens.nextToken(); 
				statusCode = Integer.parseInt(tokens.nextToken()); 
				//Get response headers from server 
				while((responseLine = s_in.readLine()).length() != 0) {
					//System.out.println(responseLine);
					String [] temp = responseLine.split(":",2);
					temp[0] = temp[0].trim().toLowerCase();
					if(temp.length == 2)
						headerMap.put(temp[0], temp[1].trim());				
				}
				
				
			}
			
			
			
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println("Message sent to "+url.getHost());
		//System.out.println("-------------------------------");
		
	}
	
	/**
	 * Redirects the client if the location header field is present
	 * @throws Exception
	 */
	public boolean sendRedirect() throws Exception {
		String location = null;
		boolean isRedirectable = false;
		if(url.getProtocol().equals("https")) {
			location = httpsCon.getHeaderField("Location");
		}
		else if(containsHeaderField("location")) {
			location = getHeaderValue("location");
		}
		if(location != null) {
			// relative redirects
			if(location.startsWith("/"))
				location = url.getHost() + location;
			// System.out.println("Redirecting to "+location);
			HTTPClient redirectedClient = new HTTPClient(location, method);
			redirectedClient.sendRequest();
			this.clone(redirectedClient);
			isRedirectable = true;
		}
		return isRedirectable;
	}
	
	/**
	 * get the redirect link from the response header if present
	 * @return null if location header is not present or absolute URL
	 */
	public String getRedirectLink() {
		String location = null;
		if(containsHeaderField("location")) {
			location = getHeaderValue("location");
			// relative redirects
			if(location.startsWith("/"))
				location = url.getProtocol() + "://"+url.getHost() + location;
			else if( !location.startsWith("www.") && ! location.startsWith("http")) {
				location = url.getProtocol() + "://"+url.getHost() + location;
			} else if(!location.startsWith("http")) {
				location = "http://" + location; 				
			}
		}
		return location;
	}
	
	public HashMap<String, String> getHeaderMap() {
		return headerMap;
	}
	
	/**
	 * returns the value of the header field in the response
	 * @param key
	 * @return
	 */
	public String getHeaderValue(String key) {
		if(headerMap.containsKey(key)) {
			return headerMap.get(key);
		} else {
			return null;
		}
	}
	
	public boolean containsHeaderField(String key) {
		return headerMap.containsKey(key);
	}
	
	/**
	 * Create a key value pair which is to be sent in the request header
	 * @param key
	 * @param value
	 * @throws Exception 
	 */
	public void setRequestHeader(String key, String value) throws Exception {
		if(requestSent == true)
			throw new Exception("HTTP Request already sent. Can't set the header now.");
		if(url.getProtocol().equalsIgnoreCase("https")) {			
			httpsCon.setRequestProperty(key, value);
		}
		else
			requestHeader.put(key, value);
	}
	
	

	public BufferedReader getReader() throws Exception {
		if(statusCode == 200 && isBodyRead == false) {
			isBodyRead = true;
			return s_in;
		}
		else {
			throw new Exception("Response code is not 200 or body has already been read. Can't read body");
		}
	}

	/**
	 * Read the body using the socket's reader
	 * @return body as string or null is reader has already been used
	 * @throws Exception
	 */
	public String getBody() throws Exception {
		String responseBody = "";
		if(requestSent == true && statusCode == 200 && isBodyRead == false) {
			isBodyRead = true;
			String responseLine = "";
			while((responseLine = s_in.readLine()) != null) {
				responseBody += responseLine +"\n";
			}
		}
		return responseBody;
	}
	
	/**
	 * Set the POST request Body
	 * @param body
	 */
	public void setBody(String body) {
		if(requestSent == true)
			try {
				throw new Exception("Cannot set parameters once the request has been sent");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		isBodySet = true;
		this.body = body;
	}
	
	/**
	 * set post request parameters.
	 * @throws Exception 
	 *  
	 */
	public void setParameter(String key, String value) throws Exception {
		if(requestSent == true)
			throw new Exception("Cannot set parameters once the request has been sent");
		parameterMap.put(key, value);
	}

		
	public static void main(String [] args) throws Exception {
		/*HTTPClient client = new HTTPClient("http://localhost:8080/HW3/status", "POST" );
		HTTPClient client = new HTTPClient("https://dbappserv.cis.upenn.edu/crawltest.html", "GET");
		client.setRequestHeader("If-Modified-Since", "Sat, 28 Mar 2015 01:54:37 GMT");
		client.setParameter("mykey1", "myvalue1");
		client.setParameter("mykey2", "myvalue2");
		client.sendRequest();
		System.out.println("request sent");
		System.out.println("request body: "+client.getBody());*/
		
		//System.out.println(new BigDecimal("10000000000000000000000000000000000000000"));
		HTTPClient client = new HTTPClient("http://localhost:8080/HW3/pushdata", "POST");
		client.setBody("my name is sheila \n sheila ki jawaani \n I'm too sexy for youuuuu");
		client.sendRequest();
	}
}
