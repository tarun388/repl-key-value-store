package com.playground;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.javalin.Javalin;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class Node {

    private static final OkHttpClient client = new OkHttpClient().newBuilder()
    .readTimeout(2, TimeUnit.SECONDS)
    .build();

    public static void main(String[] args) {
        String myId = System.getenv().getOrDefault("NODE_ID", "unknown");
        String peerEnv = System.getenv().getOrDefault("PEERS", "");
        List<String> peers = peerEnv.isEmpty() ? List.of() : Arrays.asList(peerEnv.split(","));
        int port = 7000;

        System.out.println("--- STARTING NODE " + myId + " ---");
        System.out.println("--- PEERS: " + peers + " ---");

        Javalin app = Javalin.create().start(port);

        app.get("/ping", ctx -> {
            ctx.result("Pong  from " + myId);
        });

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(2000);
                    for (String peer: peers) {
                        pingPeer(peer, myId);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start(); 
    }

    private static void pingPeer(String peerHostname, String myId) {
        String url = "http://" + peerHostname + ":7000/ping";
        Request request = new Request.Builder().url(url).build();

        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                System.out.println("[" + myId + "] -> " + peerHostname + ": SUCCESS");
            } else {
                System.out.println("[" + myId + "] -> " + peerHostname + ": FAILED (Status " + response.code() + ")");
            }
        } catch (IOException e) {
            System.err.println("[" + myId + "] -> " + peerHostname + ": UNREACHABLE (Network Error)");
        }
    }

}