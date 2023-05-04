package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import jdk.nashorn.internal.runtime.FunctionScope;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;

    /**
     * @param requestParams:
     * @return cn.itcast.hotel.pojo.PageResult
     * @throws
     * @Description TODO
     * @author Ni_cats
     * @email Ni_cats@163.com
     * @date 2023/4/28 19:15
     */

    @Override
    public PageResult search(RequestParams requestParams) {

        try {
            //1.准备request
            SearchRequest request = new SearchRequest("hotel");
            //2.准备DSL
            BuildBasicQuery(requestParams, request);

            //2.2分页
            int page = requestParams.getPage();
            Integer size = requestParams.getSize();
            request.source().from((page - 1) * size).size(size);
            request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));

            //2.3排序
            String location = requestParams.getLocation();
            if (location != null && !location.equals("")) {
                request.source().sort(SortBuilders
                        .geoDistanceSort("location", new GeoPoint(location))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS)
                );
            }
            //3.发送请求获取结果
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //4.解析响应
            return handleResponse(response);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * @param :
     * @return java.util.Map<java.lang.String, java.util.List < java.lang.String>>
     * @throws
     * @Description TODO 从数据库中进行聚合查询
     * @author Ni_cats
     * @email Ni_cats@163.com
     * @date 2023/4/28 19:15
     */

    @Override
    public Map<String, List<String>> filters(RequestParams requestParams) {
        try {
            //1.准备request
            SearchRequest request = new SearchRequest("hotel");
            //2.准备DSL

            BuildBasicQuery(requestParams, request);

            request.source().size(0);

            buildAggregation(request);
            //3.发出请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //4.解析结果
            Map<String, List<String>> result = new HashMap<>();
//        System.out.println(response);
            Aggregations aggregations = response.getAggregations();

            //根据名称获取品牌的结果
            List<String> brandList = getAggByName(aggregations, "brandAgg");
            result.put("brand", brandList);

            //根据名称获取城市的结果
            List<String> cityList = getAggByName(aggregations, "cityAgg");
            result.put("city", cityList);

            //根据名称获取星级的结果
            List<String> starNameList = getAggByName(aggregations, "starNameAgg");
            result.put("starName", starNameList);

            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSuggestions(String prefix) {
        try {
            SearchRequest request = new SearchRequest("hotel");
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "suggestions",
                    SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(prefix)
                            .skipDuplicates(true)
                            .size(10)
            ));
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
//        System.out.println(response);
            Suggest suggest = response.getSuggest();
            CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
            List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();

            List<String> list = new ArrayList<>(options.size());
            for (CompletionSuggestion.Entry.Option option : options) {
                String text = option.getText().toString();
                //            System.out.println(text);
                list.add(text);

            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertById(Long id) {
        try {
            //  根据id查询数据库得到酒店信息
            Hotel hotel = getById(id);
            //转换文档类型
            HotelDoc hotelDoc = new HotelDoc(hotel);
            //准备request对象
            IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
            //准备json文档
            request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            //发送请求
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteById(Long id) {
        try {
            DeleteRequest request = new DeleteRequest("hotel", id.toString());
            client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * @param aggregations:
     * @return java.util.List<java.lang.String>
     * @throws
     * @Description TODO 遍历聚合结果
     * @author Ni_cats
     * @email Ni_cats@163.com
     * @date 2023/4/28 19:23
     */

    private static List<String> getAggByName(Aggregations aggregations, String aggName) {
        Terms brandTerms = aggregations.get(aggName);
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        List<String> brandList = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            brandList.add(key);
//            System.out.println(key);
        }
        return brandList;
    }

    /**
     * @param request:
     * @return void
     * @throws
     * @Description TODO 聚合条件构建封装函数
     * @author Ni_cats
     * @email Ni_cats@163.com
     * @date 2023/4/28 19:19
     */

    private static void buildAggregation(SearchRequest request) {
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(100)
        );
        request.source().aggregation(AggregationBuilders
                .terms("cityAgg")
                .field("city")
                .size(100)
        );
        request.source().aggregation(AggregationBuilders
                .terms("starNameAgg")
                .field("starName")
                .size(100)
        );
    }

    /**
     * @param requestParams:
     * @param request:
     * @return void
     * @throws
     * @Description TODO 封装查询的DSL语句
     * @author Ni_cats
     * @email Ni_cats@163.com
     * @date 2023/4/28 19:16
     */

    private void BuildBasicQuery(RequestParams requestParams, SearchRequest request) {
        //2.1 query
        //构建BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //关键字搜索
        String key = requestParams.getKey();
        if (key == null || "".equals(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        //城市条件
        String city = requestParams.getCity();
        if (city != null && !city.equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("city", city));
        }
        //品牌条件
        String brand = requestParams.getBrand();
        if (brand != null && !brand.equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("brand", brand));
        }
        //星级条件
        String starName = requestParams.getStarName();
        if (starName != null && !starName.equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("starName", starName));
        }
        //价格
        Integer minPrice = requestParams.getMinPrice();
        Integer maxPrice = requestParams.getMaxPrice();
        if (minPrice != null && maxPrice != null) {
            boolQuery.filter(QueryBuilders
                    .rangeQuery("price").gte(minPrice).lte(maxPrice));
        }

        //2.2算分查询query
        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        boolQuery,
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        QueryBuilders.termQuery("isAD", true),
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });


        request.source().query(functionScoreQuery);

    }

    /**
     * @param response:
     * @return cn.itcast.hotel.pojo.PageResult
     * @throws
     * @Description TODO
     * @author Ni_cats
     * @email Ni_cats@163.com
     * @date 2023/4/28 19:16
     */


    private PageResult handleResponse(SearchResponse response) {
        //4.解析响应
        SearchHits searchHits = response.getHits();
        //4.1获取总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("总条数：" + total);
        //4.2文档数组
        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> hotels = new ArrayList<>();

        //4.3遍历
        for (SearchHit hit : hits) {
            //获取文档source
            String json = hit.getSourceAsString();
            //反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);

            //获取排序值
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0) {
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }

            //获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (highlightFields != null && !highlightFields.isEmpty()) {
                //根据字段名获取高亮结果
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null) {
                    //获取高亮值
                    String name = highlightField.getFragments()[0].toString();
                    //覆盖非高亮结果
                    hotelDoc.setName(name);
                }
            }
            hotels.add(hotelDoc);
            System.out.println("查询结果：" + hotelDoc);
        }
        return new PageResult(total, hotels);
    }

}
