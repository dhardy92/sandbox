package mapreduce;

// cc ImportFromMySQL MapReduce job that reads from a file and writes into a table.
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.sql.*;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.lang.Class;


// vv ImportFromMySQL
public class ImportFromMySQL {
  // ^^ ImportFromMySQL
  private static final Log LOG = LogFactory.getLog(ImportFromMySQL.class);

  // vv ImportFromMySQL
  public static final String NAME = "ImportFromMySQL"; // co ImportFromMySQL-1-Name Define a job name for later use.
  public enum Counters { LINES };

  // ^^ ImportFromMySQL
  /**
   * Implements the <code>Mapper</code> that takes the lines from the input
   * and outputs <code>Put</code> instances.
   */
  // vv ImportFromMySQL
  static class ImportMapper
  extends Mapper<LongWritable, MyRecord, ImmutableBytesWritable, Writable> {

    private byte[] family = null;
    private byte[] qualifier = null;

    // ^^ ImportFromMySQL
    /**
     * Prepares the column family and qualifier.
     *
     * @param context The task context.
     * @throws IOException When an operation fails - not possible here.
     * @throws InterruptedException When the task is aborted.
     */
    // vv ImportFromMySQL
    @Override
    protected void setup(Context context)
      throws IOException, InterruptedException {
      String column = context.getConfiguration().get("conf.column");
      byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
      family = colkey[0];
      if (colkey.length > 1) {
        qualifier = colkey[1];
      }
    }

    // ^^ ImportFromMySQL
    /**
     * Maps the input.
     *
     * @param offset The current offset into the input file.
     * @param line The current line of the file.
     * @param context The task context.
     * @throws IOException When mapping the input fails.
     */
    // vv ImportFromMySQL
    @Override
    public void map(LongWritable offset, MyRecord line, Context context) // co ImportFromMySQL-3-Map The map() function transforms the key/value provided by the InputFormat to what is needed by the OutputFormat.
    throws IOException {
      try {
        String name = line.name.toString();
        byte[] rowkey = Bytes.toBytes(line.id);; // co ImportFromMySQL-4-RowKey The row key is the MD5 hash of the line to generate a random key. TODO: Change Rowkey
        Put put = new Put(rowkey);
        put.add(family, qualifier, Bytes.toBytes(name)); // co ImportFromMySQL-5-Put Store the original data in a column in the given table.
        context.write(new ImmutableBytesWritable(rowkey), put);
        context.getCounter(Counters.LINES).increment(1);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  // ^^ ImportFromMySQL
  /**
   * Parse the command line parameters.
   *
   * @param args The parameters to parse.
   * @return The parsed command line.
   * @throws ParseException When the parsing of the parameters fails.
   */
  // vv ImportFromMySQL
  private static CommandLine parseArgs(String[] args) throws ParseException { // co ImportFromMySQL-6-ParseArgs Parse the command line parameters using the Apache Commons CLI classes. These are already part of HBase and therefore are handy to process the job specific parameters.
    Options options = new Options();
    Option o = new Option("t", "table", true,
      "table to import into (must exist)");
    o.setArgName("table-name");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("c", "column", true,
      "column to store row data into (must exist)");
    o.setArgName("family:qualifier");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("i", "input", true,
      "the directory or file to read from");
    o.setArgName("path-in-HDFS");
    o.setRequired(true);
    options.addOption(o);
    options.addOption("d", "debug", false, "switch on DEBUG log level");
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (Exception e) {
      System.err.println("ERROR: " + e.getMessage() + "\n");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(NAME + " ", options, true);
      System.exit(-1);
    }
    // ^^ ImportFromMySQL
    if (cmd.hasOption("d")) {
      Logger log = Logger.getLogger("mapreduce");
      log.setLevel(Level.DEBUG);
    }
    // vv ImportFromMySQL
    return cmd;
  }

  // ^^ ImportFromMySQL
  /**
   * Main entry point.
   *
   * @param args The command line parameters.
   * @throws Exception When running the job fails.
   */
  // vv ImportFromMySQL
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs =
      new GenericOptionsParser(conf, args).getRemainingArgs(); // co ImportFromMySQL-7-Args Give the command line arguments to the generic parser first to handle "-Dxyz" properties.
    CommandLine cmd = parseArgs(otherArgs);
    // ^^ ImportFromMSQL
    // check debug flag and other options
    if (cmd.hasOption("d")) conf.set("conf.debug", "true");
    // get details
    // vv ImportFromMySQL
    String table = cmd.getOptionValue("t");
    String input = cmd.getOptionValue("i");
    String column = cmd.getOptionValue("c");
    conf.set("conf.column", column);
    DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://localhost/mydatabase?user=root&password=toto");

    Job job = Job.getInstance(conf, "Import from file " + input + " into table " + table); // co ImportFromMySQL-8-JobDef Define the job with the required classes.
    job.setJarByClass(ImportFromMySQL.class);
    job.setMapperClass(ImportMapper.class);
    job.setOutputFormatClass(TableOutputFormat.class);
    job.setInputFormatClass(DataDrivenDBInputFormat.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Writable.class);
    job.setNumReduceTasks(0); // co ImportFromMySQL-9-MapOnly This is a map only job, therefore tell the framework to bypass the reduce step.
    DataDrivenDBInputFormat.setInput(job, MyRecord.class, input, "" /* conditions */,  "employee_id",  "employee_id", "name");
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


  static class MyRecord implements Writable, DBWritable {
    
    long id;
    String name;

    public MyRecord () {}
  
    public void readFields(DataInput in) throws IOException {
      this.id = in.readLong();
      this.name = Text.readString(in);
    }
  
    public void readFields(ResultSet resultSet)
        throws SQLException {
      this.id = resultSet.getLong(1);
      this.name = resultSet.getString(2);
    }
  
    public void write(DataOutput out) throws IOException {
      out.writeLong(this.id);
      Text.writeString(out, this.name);
    }
  
    public void write(PreparedStatement stmt) throws SQLException {
      stmt.setLong(1, this.id);
      stmt.setString(2, this.name);
    }
  }
}
// ^^ ImportFromMySQL
