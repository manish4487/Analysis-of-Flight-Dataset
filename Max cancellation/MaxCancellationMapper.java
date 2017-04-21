import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class MaxCancellationMapper extends
    Mapper<LongWritable, Text, Text, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  try
	  {   
	   
	  String line =value.toString();
	  String[] flight=line.split(",");
	  int Cancelledvalue=Integer.parseInt(flight[21]);
	  //flight[8],flight[16],flight[17]
	  
     if(Cancelledvalue==1)
	  { 
		  //System.out.println(flight[8]+" from "+flight[16]+"--"+flight[17]);
		  //String Resultkey =flight[8]+" from "+flight[16]+"--"+flight[17];
		  // context.write(new Text(Resultkey), new IntWritable(1));  
		  String Intermediatekey = flight[16]+"to"+flight[17];
		  context.write(new Text(flight[8]), new Text(Intermediatekey));
		  
	  }
       	
	  }
	  catch (Exception ex)
	  {
		  System.out.println("Exception is :"+ ex.getMessage());
	  }
    }
  }

