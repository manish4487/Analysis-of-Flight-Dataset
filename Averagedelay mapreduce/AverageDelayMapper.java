import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class AverageDelayMapper extends
    Mapper<LongWritable, Text, Text, AveragePair> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   String[]columns= value.toString().split(",");
   String uniqueCarrier= columns[8];
   try
   {
	   int deptDelay =Integer.parseInt(columns[15]);
     context.write(new Text(uniqueCarrier), new AveragePair(deptDelay,1));
   }
   catch (NumberFormatException e)
   {
   }
     
	  }
   }
  

