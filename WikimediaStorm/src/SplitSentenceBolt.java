import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class SplitSentenceBolt extends BaseBasicBolt {
    
    private List<String> keys = Arrays.asList("wikipedia", "added", "removed", "redirect", "history", "category", "references", "link", "page", "edit");

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String sentence = tuple.getString(0);
        String[] words = sentence.split("[\\s~`!@#$%^&*(-)+=_:;'\",.<>?/\\\\0-9" + "\\]\\[\\}\\{]+");
        for (String word : words) {
            if (keys.contains(word.toLowerCase())) {
                collector.emit(new Values(word.toLowerCase()));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}