import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class WikimediaStreamSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<String> queue;
    private transient Thread streamThread;
    private transient AtomicLong updateCount;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        this.queue = new LinkedBlockingQueue<>(10000);
        this.updateCount = new AtomicLong(0);

        streamThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    connectAndRead();
                } catch (Throwable t) {
                    System.err.println("[WIKI] Stream error, reconnecting in 5s: " + t.getMessage());
                    try { Thread.sleep(5000); }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
        streamThread.setDaemon(true);
        streamThread.start();
    }

    private void connectAndRead() throws Exception {
   // A background daemon thread opens a long-lived HTTP/1.1 connection to stream.wikimedia.org/v2/stream/recentchange. 
   // The server holds this connection open indefinitely, pushing new events as chunked transfer-encoded text.

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        System.err.println("[WIKI] Connecting via SSE to " + url);

        HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();

	HttpRequest request = HttpRequest.newBuilder()
	    .uri(URI.create(url))
	    .header("Accept", "text/event-stream")
	    .header("User-Agent", "StormLabDemo/1.0 (adam.przybylek@gmail.com)")
	    .build();


        HttpResponse<java.io.InputStream> response = client.send(
            request, HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() != 200) {
            String body = new String(response.body().readAllBytes());
            throw new RuntimeException("HTTP " + response.statusCode() + ": " + body);
        }

        System.err.println("[WIKI] SSE connected! Status: " + response.statusCode());

        BufferedReader reader = new BufferedReader(
            new InputStreamReader(response.body()));

        String eventType = "";
        StringBuilder dataBuffer = new StringBuilder();
        String line;

        while ((line = reader.readLine()) != null) {
            if (line.startsWith(":")) {
                continue; // heartbeat
            } else if (line.startsWith("event:")) {
                eventType = line.substring(6).trim();
            } else if (line.startsWith("data:")) {
                dataBuffer.append(line.substring(5).trim());
            } else if (line.isEmpty() && dataBuffer.length() > 0) {
                if (eventType.equals("message")) {
                    processEvent(dataBuffer.toString());
                }
                eventType = "";
                dataBuffer.setLength(0);
            }
        }
    }

    private void processEvent(String json) {
        try {
            JsonObject event = JsonParser.parseString(json).getAsJsonObject();

            // Only English Wikipedia edits
            String wiki = event.has("wiki") ? event.get("wiki").getAsString() : "";
            if (!wiki.equals("enwiki")) return;

            // Skip bot edits
            if (event.has("bot") && event.get("bot").getAsBoolean()) return;

            // Use the edit comment as the text to process
            String comment = event.has("comment") ? event.get("comment").getAsString() : "";
            String title = event.has("title") ? event.get("title").getAsString() : "";

            // Combine title and comment for richer text
            String text = title + " " + comment;
            text = text.replaceAll("\\[\\[|\\]\\]", " ")
                       .replaceAll("/\\*.*?\\*/", " ")
                       .replaceAll("\\s+", " ")
                       .trim();

            if (!text.isEmpty()) {
                long count = updateCount.incrementAndGet();
                if (count <= 10 || count % 100 == 0) {
                    System.err.println("[WIKI] Edit #" + count + ": "
                        + text.substring(0, Math.min(80, text.length())) + "...");
                }
                queue.offer(text);
            }
        } catch (Exception e) {
            System.err.println("[WIKI] Parse error: " + e.getMessage());
        }
    }

    @Override
    public void nextTuple() {
        String text = queue.poll();
        if (text == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(text));
        }
    }

    @Override
    public void close() {
        if (streamThread != null) streamThread.interrupt();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {}
    @Override
    public void fail(Object id) {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
}
