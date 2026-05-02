import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class TopNFinderBolt extends BaseBasicBolt {

    TreeMap<String, Integer> currentTopWords = new TreeMap<String, Integer>();
    private int N;
    private long intervalToReport = 20;
    private long lastReportTime = System.currentTimeMillis();

    public TopNFinderBolt(int N) {
        this.N = N;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        Integer count = tuple.getInteger(1);
        currentTopWords.put(word, count);

        if (currentTopWords.size() > N) {
            String key = findKeytoRemove(currentTopWords.entrySet().iterator());
            currentTopWords.remove(key);
        }

        // reports the top N words periodically
        if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
            String report = printMap();
            System.out.println("[TOP-N] " + report);
            collector.emit(new Values(report));
            lastReportTime = System.currentTimeMillis();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("top-N"));
    }

    public String printMap() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("\n\n**********top-words = [ ");
        for (String word : currentTopWords.keySet()) {
            stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
        }
        int lastCommaIndex = stringBuilder.lastIndexOf(",");
        stringBuilder.deleteCharAt(lastCommaIndex + 1);
        stringBuilder.deleteCharAt(lastCommaIndex);
        stringBuilder.append("]\n\n");
        return stringBuilder.toString();
    }

    private String findKeytoRemove(Iterator<Map.Entry<String, Integer>> iterator) {
        int min = Integer.MAX_VALUE;
        String minKey = "";
        while (iterator.hasNext()) {
            Map.Entry<String, Integer> entry = iterator.next();
            if (entry.getValue() < min) {
                min = entry.getValue();
                minKey = entry.getKey();
            }
        }
        return minKey;
    }
}