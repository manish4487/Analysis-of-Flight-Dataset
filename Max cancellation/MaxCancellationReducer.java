import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.util.ArrayList;

public class MaxCancellationReducer extends
    Reducer<Text, Text, Text, Text> {
	String Result;
	
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {    
      
	  HashMap<Text,Integer>words=new HashMap<Text,Integer>();  
	  for (Text value : values)     
	    {    	     	     		  	
			    Integer count=words.get(value);
			    if(count!=null)
			      	count++;
			    else
			    	count=0;
			     			    			    			  			    
			    words.put(value,count);
	    }

	  Text Flights=new Text();
	  int maxcount=0;
	    for(Map.Entry<Text,Integer> result:words.entrySet())
	       	 
	    	 if(result.getValue()>maxcount){
	    		 maxcount=result.getValue();
	    		 Flights=result.getKey();
	    	 }
	        	    
	  System.out.println("UniqueCarrier"+key+"ORG,DEST" +Flights+ "Count" +maxcount);
      context.write(key,new Text(Flights+"\t"+maxcount));
	 	                   
  }  
}
