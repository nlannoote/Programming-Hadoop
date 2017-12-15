package airtimecount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class aircountdriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
       Scan scan = new Scan();
        Job job = Job.getInstance(conf, "Count frequency of town chars in ukprice table.");
        job.setJarByClass(aircountdriver.class);
        TableMapReduceUtil.initTableMapperJob("airtime", scan, aircountmapper.class, Text.class, IntWritable.class,       job);
        job.setReducerClass(aircountreducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/hduser/airtimecount11"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
