package AllSpark;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class HotspotDetector implements Serializable{

	
	public static void main(String[] args) {
		// create Spark context with Spark configuration
			
		SparkConf conf = new SparkConf().setAppName("All_Spark_Phase3");
		JavaSparkContext sc = new JavaSparkContext(conf);

        double min_lat = 40.50, max_lat = 40.90, min_longt = -74.25, max_longt = -73.70,step = 0.01 ;
        int scaled_min_lat = 4050, scaled_max_long = 7370 ;
        int dim_x = 31, dim_y = 41, dim_z = 56;
        
        try {

	    	JavaRDD<String> inputInRDD = sc.textFile(args[0]);
	    	
	    	/*FileSystem hdfs =  FileSystem.get(new URI("hdfs://localhost:54310"), config);
	    	Path file = new Path("hdfs://localhost:54310/op.csv");
	    	if(hdfs.exists(file)){
	    		hdfs.delete(file,true);
	    	}*/
	    	
	    	PrintWriter pw = new PrintWriter(new File(args[1]+"/AllSpark_phase3_result.csv"));
	           	
	    	JavaPairRDD<String, Integer> javaPairRDD = inputInRDD.mapToPair(new PairFunction<String, String, Integer>() {
	    		  public Tuple2<String, Integer> call(String s) {
	    			  
	    			  Double longitude=0.0;
	    			  Double latitude=0.0;    			  
	    			  String[] row = s.split(",");

	    			  try{
	    				  longitude = Double.parseDouble(row[5]);
	    				  latitude = Double.parseDouble(row[6]);
	    			  }
	    			  catch(NumberFormatException e){
	    				  
	    			  }

	    	    		int longitudeIndex = 0,latitudeIndex = 0, timeIndex = 0;
	    	    		
	    	    		if(latitude >= min_lat && latitude <= max_lat && longitude >= min_longt && longitude <= max_longt){
	    	    				    	    			   	    			    	    			
	    	    			timeIndex = Integer.parseInt(row[1].split(" ")[0].split("-")[2])-1;
	    	    			latitudeIndex = (int) (Math.floor((latitude * 100) - scaled_min_lat));
	    	    			longitudeIndex = (int) (Math.ceil((longitude * -100)- scaled_max_long));
	    	    			
	    	        		return new Tuple2<String, Integer>(timeIndex+","+longitudeIndex+","+latitudeIndex, 1); 
	    	    		}
	    	    		else
	    	    			return new Tuple2<String, Integer>("Outlier", 1); }
	    		});
	    	JavaPairRDD<String, Integer> javaPairRDDCount = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
	    		  public Integer call(Integer a, Integer b) {return a + b; }
	    		});
	    	
	    	List<Tuple2<String,Integer>> list = javaPairRDDCount.collect();
	    	
	    	int threeDimensionalGrid[][][] = new int[dim_x][dim_y][dim_z];
	    	String[] row = new String[3];
	    	int latitude, longitude, time;
	    	
	    	for(int i=0;i<list.size();i++){
	    		if(!list.get(i)._1().equals("Outlier")){
	    			row = list.get(i)._1.split(",");
	    		    
	    			time = Integer.parseInt(row[0]);
	    			longitude = Integer.parseInt(row[1]);
	        		latitude = Integer.parseInt(row[2]);
	        		
	        		threeDimensionalGrid[time][latitude][longitude ] += list.get(i)._2;
	    		}
	    	}

	    	double cellCount  = dim_x * ((max_lat - min_lat)/ step) * ((max_longt - min_longt)/(step)) ;
	    	double sumX=0;
	    	double standardDeviation = 0.0;		
	    	double squaredSum = 0.0;
	    	double mean = 0.0;
	    	double g = 0.0;
	    	
	    	for(int i=0;i<threeDimensionalGrid.length;i++){
	    		for(int j=0;j<threeDimensionalGrid[0].length;j++){
	    			for(int k=0;k<threeDimensionalGrid[0][0].length;k++){
	    				
	    				sumX += threeDimensionalGrid[i][j][k];
	    				squaredSum += (threeDimensionalGrid[i][j][k] * threeDimensionalGrid[i][j][k]);
	    			}
	    		}
	    	}
	    	    	
	    	mean = sumX/cellCount;
	    	standardDeviation = Math.sqrt((squaredSum / cellCount) - (mean * mean));
	    	
	    	//System.out.println("Mean: " + mean + " Standard Deviation: "+ standardDeviation);
	    	
	    	Comparator<String> cmpr = new Comparator<String>(){
		   	      public int compare(String a, String b){
		   	    		return -1*Double.compare(Double.parseDouble(a.split(",")[3]), Double.parseDouble(b.split(",")[3]));
		   	    	}
		   	    };
	    	
	   	    PriorityQueue<String> zScoresQueue = new PriorityQueue<String>((int)cellCount,cmpr);
 
     	    for(int i=0;i<threeDimensionalGrid.length;i++){
	    		for(int j=0;j<threeDimensionalGrid[0].length;j++){
	    			for(int k=0;k<threeDimensionalGrid[0][0].length;k++){
	    				g = calculateGValue(threeDimensionalGrid,cellCount,mean,standardDeviation,i,j,k);
	    				zScoresQueue.offer(Integer.toString(j)+","+Integer.toString(k)+","+Integer.toString(i)+","+Double.toString(g));
	    			}
	    		}
	    	}
 
	    	StringBuilder sb = new StringBuilder();

	    	for(int i = 0; i< 50 ; i++){

	    		String []queueElements = zScoresQueue.poll().split(",");
	    		double lat = Double.parseDouble(queueElements[0]);
	    		lat = (lat/100)+((double)scaled_min_lat)/100;
	    		double longi = Double.parseDouble(queueElements[1]);
	    		longi = ((longi/100)+((double)scaled_max_long)/100)*(-1);
	    		lat = Math.round(lat * 100.0)/100.0;
	    		longi = Math.round(longi * 100.0)/100.0;
	    		
	    		sb.append(lat);
	    		sb.append(',');
	    		sb.append(longi);
	    		sb.append(',');
	    		sb.append(Integer.parseInt(queueElements[2]));
	    		sb.append(',');
	    		sb.append(queueElements[3]);
	    		sb.append('\n');

	    	}
	    	pw.write(sb.toString());

	    	pw.close();
	        
	        System.out.println("Output location \t"+args[1]);
	
		} catch (Exception e) {
			e.printStackTrace();
		}
        sc.stop();
		
	}
       
     public static double calculateGValue(int[][][] a, double cellCount, double mean,double standardDeviation, int currentTime, int currentRow, int currentColumn){
    	
    	double spatialWeight = 0, num = 0.0, den = 0.0, wMultX = 0.00 ;

    	int[] time = {currentTime-1,currentTime,currentTime+1};
   		int[] latitude = {currentRow-1,currentRow,currentRow+1};
   	    int[] longitude = {currentColumn-1,currentColumn,currentColumn+1};

		for(int i=0;i<3;i++){ 
			for(int j=0;j<3;j++){ 
				for(int k=0;k<3;k++){ 
					
					if(!(time[i] < 0 || time[i] >= a.length ||latitude[j] < 0 || latitude[j] >= a[0].length || longitude[k] < 0 || longitude[k] >= a[0][0].length)){ 
								spatialWeight++;
								wMultX += a[time[i]][latitude[j]][longitude[k]];
					} 
				} 
			}
		} 

    	num = wMultX - (mean*spatialWeight);
    	den = (standardDeviation )* (Math.sqrt(((cellCount * spatialWeight)-(spatialWeight * spatialWeight))/(cellCount-1)));
    	
    	return (num / den);
    }
}