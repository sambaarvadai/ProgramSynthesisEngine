package com.pee;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.pee.types.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

public class CalciteService {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final CalciteCompiler compiler = new CalciteCompiler();

  public static void main(String[] args) throws IOException {
    int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8765"));

    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.setExecutor(Executors.newFixedThreadPool(4));

    // Health check
    server.createContext("/health", exchange -> {
      sendText(exchange, 200, "ok");
    });

    // SELECT
    server.createContext("/compile/select", exchange -> {
      if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
        sendText(exchange, 405, "Method Not Allowed");
        return;
      }
      try {
        String body = readBody(exchange);
        SelectRequest req = mapper.readValue(body, SelectRequest.class);
        CompileResult result = compiler.compileSelect(req);
        sendJson(exchange, 200, result);
      } catch (Exception e) {
        sendJson(exchange, 400, new ErrorResponse(e.getMessage()));
      }
    });

    // INSERT
    server.createContext("/compile/insert", exchange -> {
      if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
        sendText(exchange, 405, "Method Not Allowed");
        return;
      }
      try {
        String body = readBody(exchange);
        InsertRequest req = mapper.readValue(body, InsertRequest.class);
        CompileResult result = compiler.compileInsert(req);
        sendJson(exchange, 200, result);
      } catch (Exception e) {
        sendJson(exchange, 400, new ErrorResponse(e.getMessage()));
      }
    });

    // UPDATE
    server.createContext("/compile/update", exchange -> {
      if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
        sendText(exchange, 405, "Method Not Allowed");
        return;
      }
      try {
        String body = readBody(exchange);
        UpdateRequest req = mapper.readValue(body, UpdateRequest.class);
        CompileResult result = compiler.compileUpdate(req);
        sendJson(exchange, 200, result);
      } catch (Exception e) {
        sendJson(exchange, 400, new ErrorResponse(e.getMessage()));
      }
    });

    // DELETE
    server.createContext("/compile/delete", exchange -> {
      if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
        sendText(exchange, 405, "Method Not Allowed");
        return;
      }
      try {
        String body = readBody(exchange);
        DeleteRequest req = mapper.readValue(body, DeleteRequest.class);
        CompileResult result = compiler.compileDelete(req);
        sendJson(exchange, 200, result);
      } catch (Exception e) {
        sendJson(exchange, 400, new ErrorResponse(e.getMessage()));
      }
    });

    server.start();
    System.out.println("Calcite service running on port " + port);
  }

  // Helpers
  private static String readBody(HttpExchange exchange) throws IOException {
    try (InputStream is = exchange.getRequestBody()) {
      return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static void sendText(HttpExchange exchange, int status, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "text/plain");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  private static void sendJson(HttpExchange exchange, int status, Object body)
      throws IOException {
    byte[] bytes = mapper.writeValueAsBytes(body);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }
}
