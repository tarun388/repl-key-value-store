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
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class Node {

    private static final Map<Integer, Long> peerLastSeen = new ConcurrentHashMap<>();

    private static final OkHttpClient client = new OkHttpClient().newBuilder()
    .readTimeout(2, TimeUnit.SECONDS)
    .build();
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

        app.get("/ping", ctx -> {
            ctx.result("Pong  from " + MY_ID);
        });

        new Thread(Node::runFailureDetector).start();
        new Thread(Node::runLeaderElection).start();
    }

    private static void runFailureDetector() {
        while (true) {
            for (String peerHost: PEER_HOSTNAMES) {
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

            int leaderId = Collections.max(aliveNodes);

            String status = (leaderId == MY_ID) ? "I AM THE LEADER" : "Leader is Node " + leaderId;
            System.out.println("--- [Status] Alive: " + aliveNodes + " | " + status + " ---");

            try { Thread.sleep(2000); } catch (InterruptedException e) {}
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