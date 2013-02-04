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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
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
  public static byte[] SEP = Bytes.toBytes("-");
  public static byte[] CF_VISITED = Bytes.toBytes("visited_tl");
  public static byte[] CF_VISITING = Bytes.toBytes("visiting_tl");
  public static byte[] CF_COUNTER = Bytes.toBytes("visits_count");
  public static byte[] QA_COUNTER = Bytes.toBytes("_counter");

  /**
   * Implements the <code>Mapper</code> that takes the lines from the input
   * and outputs <code>Put</code> instances.
   */
  static class ImportMapper
  extends Mapper<LongWritable, ConsulProfileEntry, ImmutableBytesWritable, KeyValue> {

    @Override
    protected void setup(Context context)
      throws IOException, InterruptedException {
    }


    private byte[] getRowKey(byte[] uid){
      //insert cell for visited and for visitor rowkey = "md5(userid)-userid"
      return Bytes.add(Bytes.toBytes(MD5Hash.getMD5AsHex(uid)), SEP, uid);
    }

    @Override
    public void map(LongWritable offset, ConsulProfileEntry cp, Context context) // co ImportFromMySQL-3-Map The map() function transforms the key/value provided by the InputFormat to what is needed by the OutputFormat.
    throws IOException {
      try {
        //generate visiting entry
        ImmutableBytesWritable rkl = new ImmutableBytesWritable(getRowKey(cp.LookMemberID));
        KeyValue rvl = new KeyValue(rkl.copyBytes(), CF_VISITING, Bytes.add( cp.ConsultationDate, cp.SeenMemberID ), Long.MAX_VALUE - Bytes.toLong(cp.ConsultationDate), cp.TypeConsultation );
        context.write(rkl, rvl);
        
        if (Bytes.toBoolean(cp.Visible) ) {
          //generate visited entry only for visible
          ImmutableBytesWritable rks = new ImmutableBytesWritable(getRowKey(cp.SeenMemberID));
          KeyValue rvs = new KeyValue(rks.copyBytes(), CF_VISITED, Bytes.add( cp.ConsultationDate, cp.LookMemberID ), Long.MAX_VALUE - Bytes.toLong(cp.ConsultationDate), cp.TypeConsultation );
          context.write(rks, rvs);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  static class ImportReducer
  extends TableReducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable> {
    @Override
    public void reduce(ImmutableBytesWritable row, Iterable<KeyValue> kvs, Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, Writable>.Context context)
    throws java.io.IOException, InterruptedException {
      Put p = new Put(row.copyBytes());
      int i = 0;
      byte[] rk = null;
      for (KeyValue kv: kvs) {
        p.add(kv.clone());
        if ( Bytes.compareTo(CF_VISITED, 0, CF_VISITED.length, kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength() ) == 0 ) {
          i++;
        }
      }
      p.add(CF_COUNTER,QA_COUNTER,Bytes.toBytes(i));
      context.write(new ImmutableBytesWritable(row),p);
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
    options.addOption(o);
    o = new Option("m", "mappers", true,
      "number of Map tasks");
    o.setArgName("mappers");
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
    String mtasks = cmd.getOptionValue("m");
    DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://"+ host + "/" + database + "?user=" + user + "&password=" + password);
    conf.setInt("mapred.map.tasks", Integer.parseInt(mtasks));

    Job job = Job.getInstance(conf, "Import from file " + stable + " into table " + dtable); // co ImportFromMySQL-8-JobDef Define the job with the required classes.
    job.setJarByClass(ImportFromMySQL.class);
    job.setMapperClass(ImportMapper.class);
    job.setReducerClass(ImportReducer.class);
    job.setOutputFormatClass(TableOutputFormat.class);
    job.setInputFormatClass(DataDrivenDBInputFormat.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, dtable);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);
    job.setNumReduceTasks(12);
    DataDrivenDBInputFormat.setInput(job, ConsulProfileEntry.class, stable, "" /* conditions */,  "LookMemberID",  "LookMemberID", "SeenMemberID", "ConsultationDate", "Visible", "TypeConsultation");
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  static class ConsulProfileEntry implements Writable, DBWritable {
    
    byte[] LookMemberID;
    byte[] SeenMemberID;
    byte[] ConsultationDate;
    byte[] Visible;
    byte[] TypeConsultation;

    public ConsulProfileEntry () {}
  
    public void readFields(DataInput in) throws IOException {
      this.LookMemberID = Bytes.toBytes(in.readInt());
      this.SeenMemberID = Bytes.toBytes(in.readInt());
      this.ConsultationDate = Bytes.toBytes(Long.MAX_VALUE - in.readLong());
      this.Visible = Bytes.toBytes(in.readBoolean());
      this.TypeConsultation = Bytes.toBytes(in.readInt());
    }
  
    public void readFields(ResultSet resultSet) throws SQLException {
      this.LookMemberID = resultSet.getBytes(1);
      this.SeenMemberID = resultSet.getBytes(2);
      this.ConsultationDate = Bytes.toBytes(Long.MAX_VALUE - resultSet.getTimestamp(3).getTime());
      this.Visible = resultSet.getBytes(4);
      this.TypeConsultation = resultSet.getBytes(5);
      
    }
  
    public void write(DataOutput out) throws IOException {
      out.writeInt(Bytes.toInt(this.LookMemberID));
      out.writeInt(Bytes.toInt(this.SeenMemberID));
      out.writeLong(Long.MAX_VALUE - Bytes.toLong(this.ConsultationDate));
      out.writeBoolean(Bytes.toBoolean(this.Visible));
      out.writeInt(Bytes.toInt(TypeConsultation));
    }
  
    public void write(PreparedStatement stmt) throws SQLException {
      stmt.setObject(1, Bytes.toInt(this.LookMemberID));
      stmt.setObject(2, Bytes.toInt(this.SeenMemberID));
      stmt.setObject(3, new Timestamp(Long.MAX_VALUE - Bytes.toLong(this.ConsultationDate)));
      stmt.setObject(4, Bytes.toBoolean(this.Visible));
      stmt.setObject(5, Bytes.toInt(this.TypeConsultation));
    }
  }
}
