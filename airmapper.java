package airtime;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;

public class airmapper extends
      TableMapper<Text, IntWritable> {
    
   private Text outChar= new Text();
    private IntWritable ONE = new IntWritable(1);
    
   public void map(ImmutableBytesWritable row, Result columns, Context context)
           throws IOException, InterruptedException {
      
	   
	   int delayed = 0;
	   String origin = new String(columns.getValue("cf1".getBytes(), "origin".getBytes()));
       String deptimestr = new String(columns.getValue("cf1".getBytes(), "deptime".getBytes()));
       String crsdeptimestr = new String(columns.getValue("cf1".getBytes(), "crsdeptime".getBytes()));
       
       if (StringUtils.isNumeric(deptimestr)
    		    && StringUtils.isNumeric(crsdeptimestr)) {
       
       int deptime = Integer.parseInt(deptimestr);
       int crsdeptime = Integer.parseInt(crsdeptimestr);
       
       if (deptime > crsdeptime){
    	   delayed = 1;
       }   
       }
       outChar.set(origin);
        context.write(outChar, new IntWritable(delayed));
    }

}