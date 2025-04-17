package com.github.abyssnlp.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class CustomerEnrichment extends RichAsyncFunction<String, String> {

    private static final long serialVersionUID = 1L;
    private final Logger logger = LoggerFactory.getLogger(CustomerEnrichment.class);

    private transient HikariDataSource dataSource;
    private transient ObjectMapper objectMapper;
    private transient ExecutorService executorService;

    private final String postgresUrl;
    private final String postgresUser;
    private final String postgresPassword;

    public CustomerEnrichment(String postgresUrl, String postgresUser, String postgresPassword) {
        this.postgresUrl = postgresUrl;
        this.postgresUser = postgresUser;
        this.postgresPassword = postgresPassword;
    }


    @Override
    public void open(OpenContext openContext) throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgresUrl);
        config.setUsername(postgresUser);
        config.setPassword(postgresPassword);
        config.setMaximumPoolSize(20);
        config.setMinimumIdle(5);
        config.setIdleTimeout(30000);
        config.setConnectionTimeout(10000);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        dataSource = new HikariDataSource(config);
        objectMapper = new ObjectMapper();
        executorService = Executors.newFixedThreadPool(30);
    }

    private JsonNode defaultCustomerEnriched(JsonNode customer) {
        ObjectNode enrichedCustomer = (ObjectNode) customer;
        enrichedCustomer.putNull("customer_name");
        enrichedCustomer.putNull("customer_age");
        enrichedCustomer.putNull("customer_address");
        enrichedCustomer.putNull("customer_created_at");
        return enrichedCustomer;
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    JsonNode node = objectMapper.readTree(input);
                    int customerId = node.get("customer_id").asInt();

                    try (Connection conn = dataSource.getConnection();
                         PreparedStatement stmt = conn.prepareStatement(
                                 "SELECT * from customers where id = ?"
                         )) {
                        stmt.setInt(1, customerId);
                        try (ResultSet rs = stmt.executeQuery()) {
                            if (rs.next()) {
                                ObjectNode enrichedNode = (ObjectNode) node;
                                enrichedNode.put("customer_name", rs.getString("name"));
                                enrichedNode.put("customer_age", rs.getString("age"));
                                enrichedNode.put("customer_address", rs.getString("address"));
                                enrichedNode.put("customer_created_at", rs.getTimestamp("created_at").toString());
                                return objectMapper.writeValueAsString(enrichedNode);
                            }
                        }
                    }
                    return objectMapper.writeValueAsString(defaultCustomerEnriched(node));
                } catch (Exception e) {
                    logger.error("Error in async enrichment: {}", e.getMessage());
                    return input;
                }
            }
        }, executorService).thenAccept(result -> {
            resultFuture.complete(Collections.singleton(result));
        });
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            dataSource.close();
        }
        if (executorService != null) {
            executorService.shutdown();
        }
    }
}
