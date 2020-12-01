package assignment1;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;


public class WeatherStation implements Serializable {

	private static long serialVersionUID = 1L;
	//Class Variables
	private String city;
	private List<Measurement> measurement;
	private static List<WeatherStation> stations = new ArrayList<>();

	//Constructor
	WeatherStation(String city, List<Measurement> measurement) {
		this.city = city;
		this.measurement = measurement;
	}


	//Q1 .  max temp function streams on getMeasurement to fetch the temp values between the given time in parameters
	// finds the maximum of those values using the comparator
	public double maxTemperature(int startTime, int endTime) {

		Double maxTemp = getMeasurement().stream().filter(m -> m.time >= startTime && m.time <= endTime)
				.collect(Collectors.maxBy(Comparator.comparingDouble(Measurement::getTemperature))).get().temperature;

		return maxTemp;
	}

	//Count Temperature T
	//Assignment 3 - Q1
	public static Integer countTemperatures(Double t) {

		//Creates Java Spark Context and Configuration
		SparkConf sparkConf = new SparkConf().setAppName("CountTemperature").setMaster("local[4]").set("spark.executor.memory", "1g");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//Creates a JavaRDD by parallelizing Stations object
		JavaRDD<WeatherStation> station = jsc.parallelize(stations);
		//JavaRDD creates an iterator of Measurements
		JavaRDD<Measurement> temp =
				station.flatMap((weatherStation) -> weatherStation.getMeasurement().iterator());
		//Filters temperatures within range(t-1,t+1)
		JavaRDD<Measurement> filtered = temp.filter(m -> m.getTemperature() >= t - 1 & m.getTemperature() <= t + 1);
		//Maps to Pair with t,1 for every temperature value in filtered RDD
		JavaPairRDD<Double, Integer> pairCount = filtered.mapToPair((Measurement s) -> new Tuple2<>(t, 1));
		//Reduces by key, sums up all the key values
		JavaPairRDD<Double, Integer> counts = pairCount.reduceByKey(Integer::sum);
		//Collects RDD to list of tuples
		List<Tuple2<Double, Integer>> output = counts.collect();
		//gets the total count value and returns it
		Integer totalCount = null;
		for (Tuple2<?, ?> tuple : output) {
			totalCount = Integer.parseInt(String.valueOf(tuple._2()));
		}

		return totalCount;
	}

	//Q2. Count Temperatures
	public static void countTemperatures(Double t1, Double t2, Double r) {


		//Splitting
		Map<String, Stream<Double>> tempStream = stations.parallelStream()
				.map(s -> new AbstractMap.SimpleEntry<String, Stream<Double>>(s.getCity(),
						s.getMeasurement().stream().map(Measurement::getTemperature)))
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
		//Mapping
		Map<Double, Integer> tempCount = tempStream.entrySet().stream().flatMap(e -> e.getValue())
				.filter(m -> (m >= t1 - r & m <= t1 + r) | (m >= t2 - r & m <= t2 + r))
				.map(t -> new AbstractMap.SimpleEntry<Double, Integer>(t, 1))
				.collect(Collectors.toMap(v -> v.getKey(), e -> e.getValue(), (v1, v2) -> v1 + v2));

		//Shuffling
		Map<Double, Integer> t1Count = tempCount.entrySet().stream().map(e -> {
			if (e.getKey() >= t1 - r & e.getKey() <= t1 + r) {
				return e.getValue();
			} else {
				return 0;
			}
		}).map(t -> new AbstractMap.SimpleEntry<Double, Integer>(t1, t))
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (v1, v2) -> v1 + v2));

		Map<Double, Integer> t2Count = tempCount.entrySet().stream().map(e -> {
			if (e.getKey() >= t1 - r & e.getKey() <= t1 + r) {
				return 0;
			} else {
				return e.getValue();
			}
		}).map(t -> new AbstractMap.SimpleEntry<Double, Integer>(t2, t))
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (v1, v2) -> v1 + v2));
		//Reducing
		List<Map<Double, Integer>> finalRes = Stream.of(t1Count, t2Count).map(m -> m).collect(Collectors.toList());

		System.out.println(finalRes);

	}

	public static void main(String[] args) {

		List<Measurement> meas = Arrays.asList(new Measurement(1, 15), new Measurement(12, 10), new Measurement(5, 12),
				new Measurement(2, 14.5), new Measurement(11, 30), new Measurement(7, 50), new Measurement(8, 23));

		WeatherStation ws = new WeatherStation("Edinburgh", meas);
		WeatherStation ws1 = new WeatherStation("London", meas);

		stations.add(ws);
		stations.add(ws1);
		System.out.println("Count of temperatures approximately within T : " + countTemperatures(15.5));
		System.out.println("max temp for given time range : " + ws.maxTemperature(1, 6));
		countTemperatures(50.0, 30.5, 5.0);

	}

	private String getCity() {
		return city;
	}

	private void setCity(String city) {
		this.city = city;
	}

	private List<Measurement> getMeasurement() {
		return measurement;
	}

	private void setMeasurement(List<Measurement> measurement) {
		this.measurement = measurement;
	}

}
