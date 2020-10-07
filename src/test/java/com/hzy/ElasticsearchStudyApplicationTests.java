package com.hzy;

import com.alibaba.fastjson.JSON;
import com.hzy.pojo.User;
import com.sun.corba.se.impl.interceptors.SlotTable;
import javafx.scene.control.IndexRange;
import net.minidev.json.JSONUtil;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class ElasticsearchStudyApplicationTests {

//    @Autowired
//    private RestHighLevelClient restHighLevelClient;

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    /**
     * 创建索引
     * @throws IOException
     */
    @Test
    void contextLoads() throws IOException {
        XContentBuilder mappings = JsonXContent.contentBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("name")
                            .field("type","text")
                            .field("analyzer", "ik_max_word")
                        .endObject()

                        .startObject("age")
                        .field("type","integer")
                        .endObject()
                    .endObject()
                .endObject();
        // 创建请求
        CreateIndexRequest request = new CreateIndexRequest("user_index")
                .mapping(mappings);
        // 执行请求
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse);
    }

    /**
     * 判断索引是否存在
     * @throws IOException
     */
    @Test
    void testIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("user_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 删除索引
     * @throws IOException
     */
    @Test
    void testDelete() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("user_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    /**
     * 创建文档
     */
    @Test
    void testDocument() throws IOException {
        User user = new User();
        user.setName("欢迎来到杭州");
        user.setAge(20);
        IndexRequest request = new IndexRequest("user_index");
//        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));

        // 将数据放入请求json
        request.source(JSON.toJSONString(user), XContentType.JSON);
        // 客户端发送请求
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println(indexResponse);

    }

    @Test
    void testIsExists() throws IOException {
        GetRequest request = new GetRequest("user_index", "1");
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 查询文档数据
     * @throws IOException
     */
    @Test
    void testGetDocument() throws IOException {
        GetRequest request = new GetRequest("newsblog");
        GetResponse documentFields = client.get(request, RequestOptions.DEFAULT);
        System.out.println(documentFields.getSourceAsString());// 打印文档内容
        System.out.println(documentFields);
    }


    /**
     * 更新文档数据
     * @throws IOException
     */
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("user_index", "1");
        request.timeout("1s");
        User user = new User();
        user.setName("欢迎来到杭州");
        user.setAge(22);
        request.doc(JSON.toJSONString(user),XContentType.JSON);

        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(update.status());
    }

    /**
     * 删除文档数据
     * @throws IOException
     */
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("user_index", "1");
        request.timeout("1s");
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.status());

    }

    /**
     * 批量插入
     */
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest request = new BulkRequest();
        request.timeout("10s");

        List<User> users = new ArrayList<>();
        users.add(new User("hzy1",20));
        users.add(new User("hzy2",20));
        users.add(new User("hzy3",20));
        users.add(new User("hzy4",20));

        for (int i = 0; i < users.size(); i++) {
            request.add(
                    new IndexRequest("user_index")
                    .id("" + i)
                    .source(JSON.toJSONString(users.get(i)),XContentType.JSON)
            );
        }
        BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());
    }

    /**
     * 搜索数据
     * @throws IOException
     */
    @Test
    void testSearch() throws IOException {
        SearchRequest request = new SearchRequest("user_index");

        // 构建搜索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 精确查询
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "欢迎");

        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        request.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
        System.out.println(searchResponse);

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }
}
