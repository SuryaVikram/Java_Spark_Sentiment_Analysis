
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.HashingTF;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import java.util.Arrays;
import java.util.Vector;

public class SentimentAnalysis {

    public static void main(String args[])

    {
        //2.a
        SparkConf sparkConf = new SparkConf().setAppName("LinearSVM").setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        String path = "SuryaVikram_Assignment3/src/main/resources/imdb_labelled.txt";
        final HashingTF tf = new HashingTF(100);
        JavaRDD<String> textFile = jsc.textFile(path);

        JavaRDD<LabeledPoint> data = textFile.map(r->new LabeledPoint(Integer.parseInt(r.split("\t")[1]),
                tf.transform(Arrays.asList((r.split("\t")[0]).split(" ")))));

        // Split initial RDD into two... [60% training data, 40% testing data].
        JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 10L);
        training.cache();
        JavaRDD<LabeledPoint> test = data.subtract(training);

        // Run training algorithm to build the model.
        int numIterations = 100;
        SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);

        // Clear the default threshold.
        model.clearThreshold();

        // Compute raw scores on the test set.
        JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(p ->
                new Tuple2<>(model.predict(p.features()), p.label()));

        scoreAndLabels.foreach(entry->System.out.println(entry));

        // Get evaluation metrics.
        //2b.
        BinaryClassificationMetrics metrics =
                new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
        double auROC = metrics.areaUnderROC();

        System.out.println("Area under ROC = " + auROC);

    }
}
