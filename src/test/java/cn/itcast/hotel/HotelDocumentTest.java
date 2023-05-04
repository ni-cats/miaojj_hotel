package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

/**
 * @BelongsProject: hotel-demo
 * @BelongsPackage: cn.itcast.hotel
 * @Author: Ni_cats
 * @email: Ni_cats@163.com
 * @CreateTime: 2023-04-27  10:49
 * @Description: TODO
 * @Version: 1.0
 */
@SpringBootTest
public class HotelDocumentTest {

    @Autowired
    private IHotelService hotelService;
    ;
    private RestHighLevelClient client;

    @Test
    void testInit() {
        System.out.println(client);
    }

    /**
     * @param :
     * @return void
     * @throws
     * @Description TODO 添加
     * @author Ni_cats
     * @email Ni_cats@163.com
     * @date 2023/4/27 13:26
     */


    @Test
    void testAddDocument() throws IOException {
        //  根据id查询数据库得到酒店信息
        Hotel hotel = hotelService.getById(61083);
        //转换文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);
        //准备request对象
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        //准备json文档
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        //发送请求
        client.index(request, RequestOptions.DEFAULT);
    }

    /**
     * @param :
     * @return void
     * @throws
     * @Description TODO 查询
     * @author Ni_cats
     * @email Ni_cats@163.com
     * @date 2023/4/27 13:25
     */

    @Test
    void testGetDocumentById() throws IOException {
        //准备request
        GetRequest request = new GetRequest("hotel", "61083");
        //发送请求，得到响应结果
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //解析结果
        String json = response.getSourceAsString();
        //将字符串转换成对象
        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);

    }

    /**
     * @param :
     * @return void
     * @throws
     * @Description TODO 修改
     * @author Ni_cats
     * @email Ni_cats@163.com
     * @date 2023/4/27 13:31
     */

    @Test
    void testUpdateDocument() throws IOException {

        UpdateRequest request = new UpdateRequest("hotel", "61083");

        request.doc(
                "price", "985",
                "starName", "四钻"
        );

        client.update(request, RequestOptions.DEFAULT);
    }

    /**
     * @param :
     * @return void
     * @throws
     * @Description TODO 删除
     * @author Ni_cats
     * @email Ni_cats@163.com
     * @date 2023/4/27 13 :33
     */

    @Test
    void testDeleteDocument() throws IOException {

        DeleteRequest request = new DeleteRequest("hotel", "61083");

        client.delete(request, RequestOptions.DEFAULT);
    }

    /**
     *
     * @Description TODO 批量插入
     * @param :
     * @return void
     * @author Ni_cats
     * @email Ni_cats@163.com
     * @date 2023/4/27 13:37
     * @throws
     */

    @Test
    void testBulkRequest() throws IOException {
        //批量查询
        List<Hotel> hotels = hotelService.list();

        BulkRequest request = new BulkRequest();
        for (Hotel hotel: hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }

        client.bulk(request,RequestOptions.DEFAULT);
    }

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.40.132:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }
}
