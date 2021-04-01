package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.config.HttpClientConfig.Builder;
import io.searchbox.core.*;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.aliases.*;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.apache.http.HttpHost;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by xiongfeng.bxf on 17/2/8.
 */
public class ESClient {
    private static final Logger log = LoggerFactory.getLogger(ESClient.class);

    private JestClient jestClient;

    public JestClient getClient() {
        return jestClient;
    }

    public void createClient(String endpoint,
                                   String user,
                                   String passwd,
                                   boolean multiThread,
                                   int readTimeout,
                                   boolean compression,
                                   boolean discovery) {

        JestClientFactory factory = new JestClientFactory();
        Builder httpClientConfig = new HttpClientConfig
                .Builder(endpoint)
                // .setPreemptiveAuth(new HttpHost(endpoint))
                .multiThreaded(multiThread)
                .connTimeout(30000)
                .readTimeout(readTimeout)
                .maxTotalConnection(200)
                .requestCompressionEnabled(compression)
                .discoveryEnabled(discovery)
                .discoveryFrequency(5l, TimeUnit.MINUTES);

        if (!("".equals(user) || "".equals(passwd))) {
            httpClientConfig.defaultCredentials(user, passwd);
        }

        factory.setHttpClientConfig(httpClientConfig.build());

        jestClient = factory.getObject();
    }

    public boolean indicesExists(String indexName) throws Exception {
        boolean isIndicesExists = false;
        JestResult rst = jestClient.execute(new IndicesExists.Builder(indexName).build());
        if (rst.isSucceeded()) {
            isIndicesExists = true;
        } else {
            switch (rst.getResponseCode()) {
                case 404:
                    isIndicesExists = false;
                    break;
                case 401:
                    // 无权访问
                default:
                    log.warn(rst.getErrorMessage());
                    break;
            }
        }
        return isIndicesExists;
    }
    public  List<SearchResult.Hit<JSONObject, Void>> search(String indexName, String typeName,String SearchSourceBuilder) throws Exception {
        log.info("query "+indexName+"/"+typeName+"/_search"+": "+ SearchSourceBuilder);
        List<SearchResult.Hit<JSONObject, Void>> hits = new ArrayList<>();
        if (indicesExists(indexName)) {
            Search search = new Search.Builder(SearchSourceBuilder).addIndex(indexName).addType(typeName).build();
            try {
                SearchResult result = jestClient.execute(search);
                hits = result.getHits(JSONObject.class);
            } catch (Exception e) {
                log.warn("index:{}, type:{}, search again!! error = {}", indexName, typeName, e.getMessage());
            }
        }
        return hits;
    }
    public boolean update(String indexName, String typeName, String id,String param) throws Exception {
        log.info("start update"+indexName+"/"+typeName+"/_"+": "+ param);
        Update update = new Update.Builder(param).index(indexName).type(typeName).id(id).build();
        JestResult result = jestClient.execute(update);
        if (result != null && !result.isSucceeded()) {
            throw new RuntimeException(result.getErrorMessage()+"插入更新索引失败!");
        }
        return result.isSucceeded();
    }
    public boolean batchInsert(String indexName, String typeName, List<JSONObject> paramList) throws Exception {
        Bulk.Builder bulk = new Bulk.Builder().defaultIndex(indexName).defaultType(typeName);
        // 批量插入
        for (JSONObject data : paramList) {
            bulk.addAction(new Index.Builder(data).build());
        }
        JestResult result = bulkInsert(bulk, 1);
        if (result != null && !result.isSucceeded()) {
            throw new RuntimeException(result.getErrorMessage()+"插入失败!");
        }

        return result.isSucceeded();
    }
    public boolean deleteIndex(String indexName) throws Exception {
        log.info("delete index " + indexName);
        if (indicesExists(indexName)) {
            JestResult rst = execute(new DeleteIndex.Builder(indexName).build());
            if (!rst.isSucceeded()) {
                return false;
            }
        } else {
            log.info("index cannot found, skip delete " + indexName);
        }
        return true;
    }

    public boolean createIndex(String indexName, String typeName,
                               Object mappings, String settings, boolean dynamic) throws Exception {
        JestResult rst = null;
        if (!indicesExists(indexName)) {
            log.info("create index " + indexName);
            rst = jestClient.execute(
                    new CreateIndex.Builder(indexName)
                            .settings(settings)
                            .setParameter("master_timeout", "5m")
                            .build()
            );
            //index_already_exists_exception
            if (!rst.isSucceeded()) {
                if (getStatus(rst) == 400) {
                    log.info(String.format("index [%s] already exists", indexName));
                    return true;
                } else {
                    log.error(rst.getErrorMessage());
                    return false;
                }
            } else {
                log.info(String.format("create [%s] index success", indexName));
            }
        }

        int idx = 0;
        while (idx < 5) {
            if (indicesExists(indexName)) {
                break;
            }
            Thread.sleep(2000);
            idx ++;
        }
        if (idx >= 5) {
            return false;
        }

        if (dynamic) {
            log.info("ignore mappings");
            return true;
        }
        log.info("create mappings for " + indexName + "  " + mappings);
        rst = jestClient.execute(new PutMapping.Builder(indexName, typeName, mappings)
                .setParameter("master_timeout", "5m").build());
        if (!rst.isSucceeded()) {
            if (getStatus(rst) == 400) {
                log.info(String.format("index [%s] mappings already exists", indexName));
            } else {
                log.error(rst.getErrorMessage());
                return false;
            }
        } else {
            log.info(String.format("index [%s] put mappings success", indexName));
        }
        return true;
    }

    public JestResult execute(Action<JestResult> clientRequest) throws Exception {
        JestResult rst = null;
        rst = jestClient.execute(clientRequest);
        if (!rst.isSucceeded()) {
            //log.warn(rst.getErrorMessage());
        }
        return rst;
    }

    public Integer getStatus(JestResult rst) {
        JsonObject jsonObject = rst.getJsonObject();
        if (jsonObject.has("status")) {
            return jsonObject.get("status").getAsInt();
        }
        return 600;
    }

    public boolean isBulkResult(JestResult rst) {
        JsonObject jsonObject = rst.getJsonObject();
        return jsonObject.has("items");
    }

    public boolean alias(String indexname, String aliasname, boolean needClean) throws IOException {
        GetAliases getAliases = new GetAliases.Builder().addIndex(aliasname).build();
        AliasMapping addAliasMapping = new AddAliasMapping.Builder(indexname, aliasname).build();
        JestResult rst = jestClient.execute(getAliases);
        log.info(rst.getJsonString());
        List<AliasMapping> list = new ArrayList<AliasMapping>();
        if (rst.isSucceeded()) {
            JsonParser jp = new JsonParser();
            JsonObject jo = (JsonObject)jp.parse(rst.getJsonString());
            for(Map.Entry<String, JsonElement> entry : jo.entrySet()){
                String tindex = entry.getKey();
                if (indexname.equals(tindex)) {
                    continue;
                }
                AliasMapping m = new RemoveAliasMapping.Builder(tindex, aliasname).build();
                String s = new Gson().toJson(m.getData());
                log.info(s);
                if (needClean) {
                    list.add(m);
                }
            }
        }

        ModifyAliases modifyAliases = new ModifyAliases.Builder(addAliasMapping).addAlias(list).setParameter("master_timeout", "5m").build();
        rst = jestClient.execute(modifyAliases);
        if (!rst.isSucceeded()) {
            log.error(rst.getErrorMessage());
            return false;
        }
        return true;
    }

    public JestResult bulkInsert(Bulk.Builder bulk, int trySize) throws Exception {
        // es_rejected_execution_exception
        // illegal_argument_exception
        // cluster_block_exception
        JestResult rst = null;
        rst = jestClient.execute(bulk.build());
        if (!rst.isSucceeded()) {
            log.warn(rst.getErrorMessage());
        }
        return rst;
    }

    /**
     * 关闭JestClient客户端
     *
     */
    public void closeJestClient() {
        if (jestClient != null) {
            jestClient.shutdownClient();
        }
    }
}
