package com.playground;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.javalin.Javalin;
import io.javalin.http.Context;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class Node {

    private static final Map<String, String> kvStore = new ConcurrentHashMap<>();

    private static final Map<Integer, Long> peerLastSeen = new ConcurrentHashMap<>();
    private static volatile int currentLeaderId = -1;

    private static final OkHttpClient client = new OkHttpClient().newBuilder().readTimeout(2, TimeUnit.SECONDS).build();
    private static Integer MY_ID;
    private static List<String> PEER_HOSTNAMES;

    public static void main(String[] args) {
        MY_ID = Integer.parseInt(System.getenv().getOrDefault("NODE_ID", "1"));
        String peerEnv = System.getenv().getOrDefault("PEERS", "");
        PEER_HOSTNAMES = peerEnv.isEmpty() ? List.of() : Arrays.asList(peerEnv.split(","));
        int port = 7000;

        System.out.println("--- STARTING NODE " + MY_ID + " ---");
        System.out.println("--- PEERS: " + PEER_HOSTNAMES + " ---");

        Javalin app = Javalin.create().start(port);

        // --- ENDPOINTS ---

        // 1. Health Check
        app.get("/ping", ctx -> {
            ctx.result("Pong  from " + MY_ID);
        });

        // 2. Client Write Request
        app.post("/data/{key}/{value}", ctx -> {
            handleWriteRequest(ctx);
        });

        // 3. Client Read Request
        app.get("/data/{key}", ctx -> {
            String value = kvStore.get(ctx.pathParam("key"));
            if (value == null) {
                ctx.status(404).result("Key not found on Node " + MY_ID);
            } else {
                ctx.result(value);
            }
        });

        // 4. Internal Replication Request from Leader
        app.post("/internal/replicate/{key}/{value}", ctx -> {
            String key = ctx.pathParam("key");
            String value = ctx.pathParam("value");
            kvStore.put(key, value);
            System.out.println("--- [Replication] Leader told me to save " + key + "=" + value + " ---");
            ctx.status(200);
        });

        // --- BACKGROUND TASKS ---
        new Thread(Node::runFailureDetector).start();
        new Thread(Node::runLeaderElection).start();
    }

    private static void replicateToPeer(String peerHost, String key, String value) {
        String url = "http://" + peerHost + ":7000/internal/replicate/" + key + "/" + value;
        Request request = new Request.Builder().url(url).post(RequestBody.create(new byte[0])).build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                System.out.println("Failed to replicate to " + peerHost);
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                response.close();
            }
        });
    }

    private static void handleWriteRequest(Context ctx) {
        String key = ctx.pathParam("key");
        String value = ctx.pathParam("value");

        if (currentLeaderId != MY_ID) {
            if (currentLeaderId == -1) {
                ctx.status(503).result("No leader elected yet. Try again later.");
                return;
            }
            int leaderHostPort = 7000 + currentLeaderId;
            String url = "http://localhost:" + leaderHostPort + "/data/" + key + "/" + value;
            ctx.status(403).redirect(url);
            System.out.println(
                    "--- [Redirect] Redirecting client to Leader (Node " + currentLeaderId + ") at " + url + " ---");
            return;
        }

        System.out.println("--- [Write] I am Leader. Saving " + key + "=" + value + " ---");

        kvStore.put(key, value);

        for (String peerHost : PEER_HOSTNAMES) {
            replicateToPeer(peerHost, key, value);
        }
    }

    private static void runFailureDetector() {
        while (true) {
            for (String peerHost : PEER_HOSTNAMES) {
                int peerId = Integer.parseInt(peerHost.split("-")[1]);
                if (pingPeer(peerHost)) {
                    peerLastSeen.put(peerId, System.currentTimeMillis());
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
        }
    }

    private static void runLeaderElection() {
        while (true) {
            long now = System.currentTimeMillis();
            List<Integer> aliveNodes = new ArrayList<>();
            aliveNodes.add(MY_ID);
            peerLastSeen.forEach((id, lastSeen) -> {
                if (now - lastSeen < 5000) {
                    aliveNodes.add(id);
                }
            });

            int newLeader = Collections.max(aliveNodes);

            if (newLeader != currentLeaderId) {
                currentLeaderId = newLeader;
                if (currentLeaderId == MY_ID) {
                    System.out.println("--- I am elected as the new Leader (Node " + MY_ID + ") ---");
                } else {
                    System.out.println("--- New Leader elected: Node " + currentLeaderId + " ---");
                }
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }
    }

    private static boolean pingPeer(String peerHostname) {
        String url = "http://" + peerHostname + ":7000/ping";
        Request request = new Request.Builder().url(url).build();

        try (Response response = client.newCall(request).execute()) {
            return response.isSuccessful();
        } catch (IOException e) {
            return false;
        }
    }

}