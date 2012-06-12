Huahin is simple Java framework for Hadoop MapReduce.
Huahin can developement easily than normal MapReduce.
Huahin is unlike Hive and Pig, are purpose to be develop flexible native Java MapReduce.


-----------------------------------------------------------------------------
Prerequisites
Huahin is based on MapReduce programs written on 0.20.x, 0.22.x, 1.0.x version(MRv1) of Hadoop.

-----------------------------------------------------------------------------
Overview
Huahin is a framework that wraps the MapReduce. If there is a lack of features,
you can also write directly because it is written in Java.

Later in this tutorial will introduce sprinkled with a simple example.

Details or run of MapReduce, please refer to the tutorial of MapReduce.(http://hadoop.apache.org/mapreduce/)

-----------------------------------------------------------------------------
Example: URL path page view

This example is intended to summarize the page views of the path from the log of the Web Server.
Data format is "USER, DATE, REFERRER, URL" in TSV format.

Source Code: PathFileter.java
public class PathFileter extends Filter {
    @Override
    public void init() {
    }

    @Override
    public void filter(Record record, Writer writer)
            throws IOException, InterruptedException {
        String url = record.getValueString("URL");
        URI uri = URI.create(url);
        String path = uri.getPath();
        if (path == null) {
            return;
        }

        String user = record.getValueString("USER");
        String date = StringUtil.split(record.getValueString("DATE"), " ", true)[0];

        Record emitRecord = new Record();
        emitRecord.addGrouping("DATE", date);
        emitRecord.addGrouping("PATH", path);
        emitRecord.addValue("USER", user);
        writer.write(emitRecord);
    }

    @Override
    public void filterSetup() {
    }
}

Filter is an abstract class change to Mapper. Filter class is processed by filter method.
Record the parameters of the filter method will contain the Value and Key.
To get the value specifies the type and label. Writer is used to write the Record.

Record using the Writer to write, you specify groups, values, and the sort.

Filter#init method is called before each filter is called.

Filter#fileterSetup method is called once at the beginning of the task.


Source Code: PathSummarizer.java
public class PathSummarizer extends Summarizer {
    @Override
    public void init() {
    }

    @Override
    public void summarizer(Writer writer)
            throws IOException, InterruptedException {
        int pv = 0;
        while (hasNext()) {
          next(writer);
          pv++;
        }

        Record emitRecord = new Record();
        emitRecord.addValue("PV", pv);
        writer.write(emitRecord);
    }

    @Override
    public void summarizerSetup() {
    }
}

Summarizer is an abstract class change to Reducer. Summarizer class is processed by summarizer method.
Call the Summarizer#next method in Summarizer, to get the Record.
The end of the Record decision, use the Summarizer#hasNext method.
This specification is similar to the iterator of Java.

Record the parameters of the summarizer method will contain the Value and Key.
To get the value specifies the type and label. Writer is used to write the Record.
Record using the Writer to write, you specify groups, values, and the sort.

Summarizer#init method is called for each group.

Summarizer#summarizerSetup method is called once at the beginning of the task.


Source Code: PathPVJobTool.java
public class PathPVJobTool extends SimpleJobTool {
    @Override
    protected String setInputPath(String[] args) {
        return args[0];
    }

    @Override
    protected String setOutputPath(String[] args) {
        return args[1];
    }

    @Override
    protected void setup() throws Exception {
        final String[] labels = new String[] { "USER", "DATE", "REFERER", "URL" };

        SimpleJob job = addJob(labels, StringUtil.TAB);
        job.setFilter(PathFileter.class);
        job.setSummaizer(PathSummarizer.class);
    }
}

To configure the Job, you will create a tool that inherit from this class.
Set the input path of HDFS in SimpleJobTool#setInputPath method.
Set the output path of HDFS in SimpleJobTool#setOutputPath method.

Set up a job in the SimpleJobTool#setup method.
Add a job in the addJob method, set the Fileter in setFilter, to set the Summarizer in setSummaizer.

For more information, check the Javadoc.


Source Code: Jobs.java
public class Jobs {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("[jobName] args...");
            System.exit(-1);
        }

        // Remove the leading argument and call run
        String jobName = args[0];
        String[] newArgs = new String[args.length - 1];
        for (int i = 1; i < args.length; ++i) {
            newArgs[i - 1] = args[i];
        }

        Runner runner = new Runner();
        runner.addJob("PathPV", PathPVJobTool.class);

        int status = runner.run(jobName, newArgs);
        System.exit(status);
    }
}

Runner can be called easily the Job. Add the name and Job in JobTool addJob method.
To run the job, you call the run out by specifying the Job name and parameters you have just registered.
In this example, delete the name of Job from the startup parameters, and set the run parameters.

For more information, check the Huahin Examples.
