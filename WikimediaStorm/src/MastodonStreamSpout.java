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

public class MastodonStreamSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<String> queue;
    private String streamingUrl;
    private String accessToken;
    private transient Thread streamThread;
    private transient AtomicLong updateCount;

    /**
     * @param streamingUrl  the streaming server, e.g. "https://streaming.mastodon.social"
     * @param accessToken   OAuth token from mastodon.social/settings/applications
     */
    public MastodonStreamSpout(String streamingUrl, String accessToken) {
        this.streamingUrl = streamingUrl;
        this.accessToken = accessToken;
    }

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
                    System.err.println("[MASTODON] Stream error, reconnecting in 5s: " + t);
                    t.printStackTrace(System.err);
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
        String url = streamingUrl + "/api/v1/streaming/public";
        System.err.println("[MASTODON] Connecting via SSE to " + url);

        HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Authorization", "Bearer " + accessToken)
            .header("Accept", "text/event-stream")
            .build();

        HttpResponse<java.io.InputStream> response = client.send(
            request, HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() != 200) {
            String body = new String(response.body().readAllBytes());
            throw new RuntimeException("HTTP " + response.statusCode() + ": " + body);
        }

        System.err.println("[MASTODON] SSE connected! Status: " + response.statusCode());

        BufferedReader reader = new BufferedReader(
            new InputStreamReader(response.body()));

        // SSE format:
        // :thump                     <- heartbeat, ignore
        // event: update              <- event type
        // data: {"id":"...","content":"...",...}  <- payload
        //                            <- blank line = end of event

        String eventType = "";
        StringBuilder dataBuffer = new StringBuilder();
        String line;

        while ((line = reader.readLine()) != null) {
            if (line.startsWith(":")) {
                // heartbeat comment, ignore
                continue;
            } else if (line.startsWith("event:")) {
                eventType = line.substring(6).trim();
            } else if (line.startsWith("data:")) {
                dataBuffer.append(line.substring(5).trim());
            } else if (line.isEmpty() && dataBuffer.length() > 0) {
                // End of event — process it
                if (eventType.equals("update")) {
                    processUpdate(dataBuffer.toString());
                }
                eventType = "";
                dataBuffer.setLength(0);
            }
        }
    }

    private void processUpdate(String json) {
        try {
            JsonObject status = JsonParser.parseString(json).getAsJsonObject();

            // Skip non-English
            if (status.has("language")
                && !status.get("language").isJsonNull()
                && !status.get("language").getAsString().equals("en")) {
                return;
            }

            // Strip HTML tags
            String content = status.get("content").getAsString()
                .replaceAll("<[^>]+>", " ")
                .replaceAll("\\s+", " ")
                .trim();

            if (!content.isEmpty()) {
                long count = updateCount.incrementAndGet();
                if (count <= 10 || count % 100 == 0) {
                    System.err.println("[MASTODON] Update #" + count + ": "
                        + content.substring(0, Math.min(80, content.length())) + "...");
                }
                queue.offer(content);
            }
        } catch (Exception e) {
            System.err.println("[MASTODON] Parse error: " + e.getMessage());
        }
    }

    @Override
    public void nextTuple() {
        String post = queue.poll();
        if (post == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(post));
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
