/*
 * DBCollector.java
 *
 * Copyright (C) 2020, Nicolas Martin
 *
 */

import org.apache.log4j.*;

import java.lang.*;
import java.io.*;
import java.util.*;

public class DBCollector
{
    private String		script;
    private int			interval;
    private String		user;
    private String		password;
    private String      cnx;
    private String      ssl;
    private File		outputDir;
    private Logger		log;

    private CollectData		collector = null;
    private Thread		collectorThread = null;
    private boolean		endCollection = false;
    private Process		collProc;

    private BufferedWriter	resultCSVs;

    public DBCollector(String script, int runID, int interval,
                       String cnx, String ssl,String user, String password, File outputDir,
                       Logger log)
    {
    List<String>	cmdLine = new ArrayList<String>();
	
    this.script	    = script;
	this.interval	= interval;
	this.cnx    	= cnx;
	this.ssl    	= ssl;	
    this.user   	= user;
	this.password	= password;
	this.log	    = log;

	cmdLine.add("python");
	cmdLine.add("-");
	cmdLine.add(Integer.toString(runID));
	cmdLine.add(Integer.toString(interval));
	cmdLine.add(cnx);
    cmdLine.add(ssl);
	cmdLine.add(user);
    cmdLine.add(password);
	cmdLine.add(Integer.toString(interval));

	try
	{
	    resultCSVs = new BufferedWriter(new FileWriter(new File(outputDir, "db_info.csv")));
	}
	catch (Exception e)
	{
	    log.error("DBCollector, " + e.getMessage());
	    System.exit(1);
	}

	try
	{
	    ProcessBuilder pb = new ProcessBuilder(cmdLine);
	    pb.redirectError(ProcessBuilder.Redirect.INHERIT);

	    collProc = pb.start();

	    BufferedReader scriptReader = new BufferedReader(new FileReader(script));
	    BufferedWriter scriptWriter = new BufferedWriter(new OutputStreamWriter(collProc.getOutputStream()));
	    String line;
	    while ((line = scriptReader.readLine()) != null)
	    {
		scriptWriter.write(line);
		scriptWriter.newLine();
	    }
	    scriptWriter.close();
	    scriptReader.close();
	}
	catch (Exception e)
	{
	    log.error("DBCollector " + e.getMessage());
	    e.printStackTrace();
	    System.exit(1);
	}

	collector = new CollectData(this);
	collectorThread = new Thread(this.collector);
	collectorThread.start();
    }

    public void stop()
    {
    	endCollection = true;
	try
	{
	    collectorThread.join();
	}
	catch (InterruptedException ie)
	{
	    log.error("DBCollector, " + ie.getMessage());
	    return;
	}
    }

    private class CollectData implements Runnable
    {
    	private DBCollector	parent;

    	public CollectData(DBCollector parent)
	{
	    this.parent = parent;
	}

	public void run()
	{
	    BufferedReader	dbData;
	    String		line;
	    int			resultIdx = 0;

	    dbData = new BufferedReader(new InputStreamReader(
	    		parent.collProc.getInputStream()));

	    while (!endCollection || resultIdx != 0)
	    {
		try
		{
		    line = dbData.readLine();
		    if (line == null)
		    {
		    	log.error("DBCollector, unexpected EOF " +
				  "while reading from external " +
				  "helper process");
		    	break;
		    }
		    parent.resultCSVs.write(line);
		    parent.resultCSVs.newLine();
		    parent.resultCSVs.flush();
		}
		catch (Exception e)
		{
		    log.error("DBCollector, " + e.getMessage());
		    break;
		}
	    }

	    try
	    {
            dbData.close();
            parent.resultCSVs.close();
	    }
	    catch (Exception e)
	    {
	    	log.error("DBCollector, " + e.getMessage());
	    }
	}
    }
}


