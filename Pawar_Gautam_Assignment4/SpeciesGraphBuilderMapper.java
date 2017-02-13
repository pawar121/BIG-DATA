// 
// Author - Jack Hebert (jhebert@cs.washington.edu) 
// Copyright 2007 
// Distributed under GPLv3 
// 
// Modified - Dino Konstantopoulos
// Distributed under the "If it works, remolded by Dino Konstantopoulos, 
// otherwise no idea who did! And by the way, you're free to do whatever 
// you want to with it" dinolicense
// 
package U.CC;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.util.*; 
import java.lang.StringBuilder; 
  
 /* 
  * This class reads in a serialized download of wikispecies, extracts out the links, and 
  * foreach link: 
  *   emits (currPage, (linkedPage, 1)) 
  * 
  * 
  */ 
 public class SpeciesGraphBuilderMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> { 
  
  
   public void map(LongWritable key, Text value, 
                   OutputCollector output, Reporter reporter) throws IOException
{
     // Prepare the input data. 
     String page = value.toString(); 
  
     System.out.println("Page:" + page); 
     String title = this.GetTitle(page, reporter); 
     if (title.length() > 0) { 
       reporter.setStatus(title); 
     } else { 
       return; 
     } 
  
     ArrayList<String> outlinks = this.GetOutlinks(page); 
     StringBuilder builder = new StringBuilder(); 
	 if(outlinks.size()>0){
	for (String link : outlinks) { 
       link = link.replace(" ", "_");
	   String link1[] = link.split(",");
	   link1[0]=link1[0].replaceAll("[a-z]{2}:.*", " ");
       builder.append(" "); 
       builder.append(link1[0]); 
     }
	 output.collect(new Text(title), new Text(builder.toString()));  
	 }
   } 
  
   public String GetTitle(String page, Reporter reporter) throws IOException{ 
            int end = page.indexOf(",");
            if (-1 == end)
                return "";
            return page.substring(0, end);
   } 
  
   public ArrayList<String> GetOutlinks(String page){ 
     int end,end1; 
     ArrayList<String> outlinks = new ArrayList<String>(); 
     int start=page.indexOf("[[");   //82
	 int start1=page.indexOf("{{");
     while (start > 0) { 
       start = start+2;    //84
       end = page.indexOf("]]", start); //99
	   
       //if((end==-1)||(end-start<0)) 
       if (end == -1) { 
         break; 
       }
	   //if(end1 ==-1){
		 //  end1 = page.indexOf(",", start1);
        //   end1 = end1 - 2;
	   //}
	   
       String toAdd = page.substring(start); 
       toAdd = toAdd.substring(0, end-start);
	   
       outlinks.add(toAdd); 
	   
       start = page.indexOf("[[", end+1); 
     }
		while(start1>0){
			start1=start1+2;
			end1= page.indexOf("}}", start1);
			if(end1 ==-1){break;}
			String toAdd1=page.substring(start1); 
			toAdd1 = toAdd1.substring(0, end1-start1);
		outlinks.add(toAdd1); 
	   start1 = page.indexOf("{{", end1+1); 
		}
     return outlinks; 
   } 
 }

