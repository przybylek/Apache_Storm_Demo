import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TwitterTopology {

    public static StormTopology createTopology(String[] keyWords) {
        TopologyBuilder builder = new TopologyBuilder();
        // builder.setSpout("spout", new MastodonStreamSpout("https://streaming.mastodon.social", "YVQuAhr3hYLFzgIwtKx4KVbnjbxSJGsrSBrh_BMVIjs"), 1);
        builder.setSpout("spout", new WikimediaStreamSpout(), 1);
        builder.setBolt("split", new SplitSentenceBolt(), 8).shuffleGrouping("spout");
        // builder.setBolt("normalize", new NormalizerBolt(), 8).shuffleGrouping("split");
        builder.setBolt("count", new WordCountBolt(), 12).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("top-n", new TopNFinderBolt(5), 1).globalGrouping("count");
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        // args[0] = "remote" or "local", args[1..n] = keywords
        String[] keyWords = (args != null && args.length > 1)
                ? Arrays.copyOfRange(args, 1, args.length)
                : new String[]{"storm", "data"};  // default keywords for testing

        StormTopology topology = createTopology(keyWords);

        Config config = new Config();
        config.setDebug(true);

        if (args != null && args.length > 0 && args[0].equals("remote")) {
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar("twitter-word-count", config, topology);
        } else {
            config.setMaxTaskParallelism(3);
            try (LocalCluster cluster = new LocalCluster()) {
                cluster.submitTopology("twitter-word-count", config, topology);
                Thread.sleep(1 * 60 * 1000); // run for 1 minutes then stop
            }
        }
    }
}