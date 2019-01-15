import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import twitter4j.Status;

public class FetchTweets {

	static Map<String, Integer> map = new HashMap<String, Integer>();

	public static void main(String[] args) throws IOException {

		String consumerKey = "MoJMkotXfIwaxP8BmCNWvHhzD";
		String consumerSecret = "bFEqIOyYiqvhJuzO35pIupf5rr8zfBnH5wL7Sk6fiKw3LOyhPE";
		String accessToken = "173806625-bgSRU2LsKaxxkkDv0VW48WpJ7fEX3FbdpodMRDIz";
		String accessTokenSecret = "DCVjhNxwhNz8R15lTOOObGduUiIvqG33TWwwd8zuhOY27";

		System.setProperty("hadoop.home.dir", "C:\\Program Files\\winutil\\");

		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

		String[] filters = new String[] { "ripple", "bitcoin", "ethereum" };

		SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("SentimentAnalysis")
				.set("spark.driver.allowMultipleContexts", "true");
		JavaStreamingContext jssc = null;
		jssc = new JavaStreamingContext(conf, new Duration(1000));
		findTweets(filters, jssc);

	}

	public static void findTweets(final String[] filter, final JavaStreamingContext jssc) {

		JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, filter);
		JavaDStream<String> statuses = twitterStream.map(new Function<Status, String>() {

			private static final long serialVersionUID = -3466492073878370481L;

			public String call(Status status) {
				return status.getText();
			}
		});

		statuses.foreachRDD(new Function<JavaRDD<String>, Void>() {
			int bitcount = 0;
			int ripcount = 0;
			int ethereumcount = 0;

			List<String> bitlist = new ArrayList<String>();
			List<String> riplist = new ArrayList<String>();
			List<String> ethereumlist = new ArrayList<String>();

			FetchTweets ft = new FetchTweets();

			public Void call(JavaRDD<String> rdd) throws Exception {
				if (rdd != null) {
					List<String> result = rdd.collect();

					if (bitcount >= 20 && ripcount >= 20 && ethereumcount >= 20) {
						
						System.out.println("\n-----------------------------------------------------------\n");
						System.out.println("The Most Popular Cryptocurrencies according to sentiments are:");
						System.out.println("\n-----------------------------------------------------------\n");
						sortCurrency(map);
						System.out.println("\n-----------------------------------------------------------\n");

						jssc.stop(true, true);
						return null;
					}

					for (String s : result) {

						if (s.contains("bitcoin") && bitcount <= 20) {
							bitcount++;
							bitlist.add(s);
							if (bitcount == 20) {
								ft.sentimentAnalyser(bitlist, "bitcoin");
							}
						}

						if (s.contains("ripple") && ripcount <= 20) {
							ripcount++;
							riplist.add(s);
							if (ripcount == 20) {
								ft.sentimentAnalyser(riplist, "ripple");
							}
						}

						if (s.contains("ethereum") && ethereumcount <= 20) {
							ethereumcount++;
							ethereumlist.add(s);
							if (ethereumcount == 20) {
								ft.sentimentAnalyser(ethereumlist, "ethereum");
							}
						}

					}

				}

				return null;
			}

		});

		jssc.start();
		jssc.awaitTermination();

	}

	public static void sortCurrency(Map<String, Integer> currencyMap) {

		List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(currencyMap.entrySet());

		// Sort the list
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		Collections.reverse(list);

		HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, Integer> aa : list) {
			temp.put(aa.getKey(), aa.getValue());
		}

		for (Map.Entry<String, Integer> vals : temp.entrySet()) {
			System.out.println("Currency Name:" + vals.getKey() + " || Sentiment Value:" + vals.getValue());
		}

	}

	public void sentimentAnalyser(List<String> list, String filter) {

		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		int mainSentiment = 0;
		int finalSentiment = 0;
		for (String line : list) {
			if (line != null && line.length() > 0) {
				int longest = 0;
				Annotation annotation = pipeline.process(line);
				for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
					Tree tree = sentence.get(SentimentAnnotatedTree.class);
					int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
					String partText = sentence.toString();
					if (partText.length() > longest) {
						mainSentiment = sentiment;
						longest = partText.length();
					}

				}
			}
			finalSentiment += mainSentiment;
		}
		System.out.println("-----------------------------------------------------------");
		System.out.println("\n\nSentiment value for " + filter + " :" + finalSentiment + "\n\n");
		System.out.println("-----------------------------------------------------------");

		if (map.containsKey(filter) == false) {
			map.put(filter, finalSentiment);
		}
	}


}
