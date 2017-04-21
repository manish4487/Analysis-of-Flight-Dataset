package uis.bigdataclass.MostFrequentDestination;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MostFrequentDestMapper extends
Mapper<LongWritable, Text, Text, Text> {
public void map(LongWritable key, Text value, Context context)
  throws IOException, InterruptedException {
  String[] COLUMNS = value.toString().split(",");
  
  try{
		  String Carrier_Org=COLUMNS[8]+" "+COLUMNS[16];
		  String Dest = COLUMNS[17];
		  context.write(new Text(Carrier_Org),new Text(Dest)); 
  }catch(InterruptedException e)
  {
  }

 }

}
