package airtimecount;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;

public class aircountmapper extends
      TableMapper<Text, IntWritable> {
    
   private Text outChar= new Text();
    private IntWritable ONE = new IntWritable(1);
    
   public void map(ImmutableBytesWritable row, Result columns, Context context)
           throws IOException, InterruptedException {
        
       String charAtoZ = new String(columns.getValue("cf1".getBytes(), "origin".getBytes()));
        outChar.set(charAtoZ);
        context.write(outChar, ONE);
    }

}