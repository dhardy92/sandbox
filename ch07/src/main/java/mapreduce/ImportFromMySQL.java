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
import org.apache.hadoop.hbase.client.Increment;
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
import org.apache.hadoop.hbase.util.MD5Hash;
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
  extends Mapper<LongWritable, ConsulProfileEntry, ImmutableBytesWritable, Writable> {


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
    public void map(LongWritable offset, ConsulProfileEntry cp, Context context) // co ImportFromMySQL-3-Map The map() function transforms the key/value provided by the InputFormat to what is needed by the OutputFormat.
    throws IOException {
      try {
        //insert cell for visited and for visitor rowkey = "md5(userid)-userid"
        byte[] rowkeyl = Bytes.add(Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(cp.LookMemberID))), Bytes.toBytes("-"), Bytes.toBytes(cp.SeenMemberID));
        byte[] rowkeys = Bytes.add(Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(cp.SeenMemberID))), Bytes.toBytes("-"), Bytes.toBytes(cp.SeenMemberID));
        Put putl = new Put(rowkeyl);
        Put puts = new Put(rowkeys);
        putl.add(Bytes.toBytes("visiting_tl"), Bytes.add(Bytes.toBytes(Long.MAX_VALUE - cp.ConsultationDate.getTime()), Bytes.toBytes(cp.SeenMemberID)), Bytes.toBytes(cp.TypeConsultation));
        puts.add(Bytes.toBytes("visited_tl"), Bytes.add(Bytes.toBytes(Long.MAX_VALUE - cp.ConsultationDate.getTime()), Bytes.toBytes(cp.LookMemberID)), Bytes.toBytes(cp.TypeConsultation));
        context.write(new ImmutableBytesWritable(rowkeyl), putl);
        context.write(new ImmutableBytesWritable(rowkeys), puts);
        //inc num of MySQL tupple managed
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
    Option o = new Option("s", "stable", true,
      "MySQL table to import from");
    o.setArgName("MySQL-table-name");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("b", "database", true,
      "MySQL Database where is to import from");
    o.setArgName("database");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("d", "dtable", true,
      "HBase table to import to (must exists)");
    o.setArgName("HBase-table");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("h", "host", true,
      "MySQL host");
    o.setArgName("localhost");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("u", "user", true,
      "MySQL user");
    o.setArgName("user");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("p", "password", true,
      "MySQL password");
    o.setArgName("password");
    o.setRequired(true);
    options.addOption(o);

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
    String stable = cmd.getOptionValue("s");
    String database = cmd.getOptionValue("b");
    String dtable = cmd.getOptionValue("d");
    String host = cmd.getOptionValue("h");
    String user = cmd.getOptionValue("u");
    String password = cmd.getOptionValue("p");
    DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://"+ host + "/" + database + "?user=" + user + "&password=" + password);
    conf.setInt("mapred.map.tasks", 12);

    Job job = Job.getInstance(conf, "Import from file " + stable + " into table " + dtable); // co ImportFromMySQL-8-JobDef Define the job with the required classes.
    job.setJarByClass(ImportFromMySQL.class);
    job.setMapperClass(ImportMapper.class);
    job.setOutputFormatClass(TableOutputFormat.class);
    job.setInputFormatClass(DataDrivenDBInputFormat.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, dtable);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Writable.class);
    job.setNumReduceTasks(0); // co ImportFromMySQL-9-MapOnly This is a map only job, therefore tell the framework to bypass the reduce step.
    DataDrivenDBInputFormat.setInput(job, ConsulProfileEntry.class, stable, "" /* conditions */,  "LookMemberID",  "LookMemberID", "SeenMemberID", "ConsultationDate", "Visible", "Referer", "TypeConsultation");
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


  static class ConsulProfileEntry implements Writable, DBWritable {
    
    long LookMemberID;
    long SeenMemberID;
    Timestamp ConsultationDate;
    boolean Visible;
    String Referer;
    int TypeConsultation;

    public ConsulProfileEntry () {}
  
    public void readFields(DataInput in) throws IOException {
      this.LookMemberID = in.readLong();
      this.SeenMemberID = in.readLong();
      this.ConsultationDate.setTime(in.readLong());
      this.Visible = in.readBoolean();
      this.Referer = in.readUTF();
      this.TypeConsultation = in.readInt();
    }
  
    public void readFields(ResultSet resultSet) throws SQLException {
      this.LookMemberID = resultSet.getLong(1);
      this.SeenMemberID = resultSet.getLong(2);
      this.ConsultationDate = resultSet.getTimestamp(3);
      this.Visible = resultSet.getBoolean(4);
      this.Referer = resultSet.getString(5);
      this.TypeConsultation = resultSet.getInt(6);
      
    }
  
    public void write(DataOutput out) throws IOException {
      out.writeLong(this.LookMemberID);
      out.writeLong(this.SeenMemberID);
      out.writeLong(this.ConsultationDate.getTime());
      out.writeBoolean(this.Visible);
      Text.writeString(out,this.Referer);
      out.writeInt(TypeConsultation);
    }
  
    public void write(PreparedStatement stmt) throws SQLException {
      stmt.setObject(1, this.LookMemberID);
      stmt.setObject(2, this.SeenMemberID);
      stmt.setObject(3, this.ConsultationDate);
      stmt.setObject(4, this.Visible);
      stmt.setObject(5, this.Referer);
      stmt.setObject(6, this.TypeConsultation);
    }
  }
}
// ^^ ImportFromMySQL
