package org.ymlakes.example;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ESDemo {
    TransportClient client = null;
    @Before
    public void testConnect()throws UnknownHostException{
        Settings settings = Settings.builder().put("cluster.name","my-application").build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("xman-tech.cn"), Integer.parseInt("9300")));
    }

    @Test
    public void testSearch() {
        GetResponse response = client.prepareGet("accounts","person","1")
                .execute().actionGet();
        System.out.println(response.getSourceAsString());
    }
    /**
     * 使用es的帮助类
     */
    public XContentBuilder createJson() throws Exception {
        // 创建json对象, 其中一个创建json的方式
        XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("user", "songweiyi")
                .field("title", "java工程师")
                .field("desc", "一名初级网站开发工程师")
                .endObject();
        return source;
    }
    @Test
    public void testAdd()throws Exception{
        XContentBuilder source = createJson();
        // 存json入索引中
        IndexResponse response = client.prepareIndex("accounts", "person", "4").setSource(source).get();
        // 结果获取
        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        //如果是CREATED代表添加成功
        RestStatus status = response.status();
        System.out.println(index + " : " + type + ": " + id + ": " + version + ": " + status);
    }

    @Test
    public void testDelete(){
        DeleteResponse response = client.prepareDelete("accounts","person","2").get();
        //if return ok so delete success
        System.out.println(response.status());
    }

    @Test
    public void testUpdate()throws IOException {
        UpdateRequest request = new UpdateRequest("accounts","person","5");
        request.doc(
                XContentFactory.jsonBuilder()
                .startObject()
                //.field("user", "kuluoluo")
                .field("title", "php")
                //.field("desc", "一名初级网站开发工程师")
                .endObject());
        UpdateResponse response = client.update(request).actionGet();
        System.out.println(response.status());

    }
    //如果没有侧添加，有的话就更新
    @Test
    public void testUpset(){

    }


    @Test
    public void testMgetBatchSearch(){
        MultiGetResponse responses = client.prepareMultiGet()
                .add("accounts","person","1","3","4")
                .get();
        for(MultiGetItemResponse itemResponse:responses){
            GetResponse response = itemResponse.getResponse();
            System.out.println(response.getSourceAsString());
        }
    }

    @Test
    public void testBulkBatchAdd()throws Exception{
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(client.prepareIndex("accounts","person","2")
        .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .field("user", "ymlakes")
                .field("title", "php")
                .field("desc", "a soft enger")
                .endObject()));
        bulkRequestBuilder.add(client.prepareIndex("accounts","person","6")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("user", "ymlakes")
                        .field("title", "python")
                        .field("desc", "一名初级网站开发工程师")
                        .endObject()));
        BulkResponse responses = bulkRequestBuilder.get();
        System.out.println(responses.status());
    }

    @Test
    public void testQueryAndDelete(){

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("user","lixin"))
                .source("accounts").get();
        //代表删除文档的个数
        long count = response.getDeleted();
        System.out.println(count);
    }

    @Test
    public void testQueryAll(){
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(queryBuilder)
                .setSize(4).get();//每次查询三个
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }

    }

    @Test
    public void testMatchQuery(){
        //查询条件
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("title","java");
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(queryBuilder)
                .setSize(4).get();//每次查询三个
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }
    @Test
    public void testMultiMatchQuery(){
        //查询条件
        QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery("java","title","desc");
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(queryBuilder)
                .setSize(4).get();//每次查询三个
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    @Test
    public void testTermQuery(){
        QueryBuilder queryBuilder = QueryBuilders.termQuery("title","java");
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(queryBuilder)
                .setSize(4).get();//每次查询三个
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    @Test
    public void testTermsQuery(){
        QueryBuilder queryBuilder = QueryBuilders.termsQuery("title","java","php");
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(queryBuilder)
                .setSize(4).get();//每次查询三个
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    @Test
    public void testRangeQuery(){
        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("weight")
                .gt("1");
        SearchResponse response = client.prepareSearch("book")
                .setQuery(queryBuilder)
                .setSize(4).get();//每次查询三个
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    @Test
    public void testPrefixQuery(){
        QueryBuilder queryBuilder = QueryBuilders.prefixQuery("user","张");
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(queryBuilder)
                .setSize(4).get();//每次查询三个
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }
    //模糊查询
    @Test
    public void testWildCardQuery(){
        //QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("user","张*");
        QueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("tilte","java");
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(queryBuilder)
                .setSize(4).get();//每次查询三个
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    //类型查询
    @Test
    public void testTypeQuery(){
        QueryBuilder queryBuilder = QueryBuilders.typeQuery("person");
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(queryBuilder)
                .setSize(4).get();//每次查询三个
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }
    //id查询
    @Test
    public void testIDQuery(){
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1","3");
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(queryBuilder)
                .setSize(4).get();//每次查询三个
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    //聚合查询
    @Test
    public void testAggsQuery(){
        //求最大值
//        AggregationBuilder builder = AggregationBuilders.max("weightMax").field("weight");
//        SearchResponse response = client.prepareSearch("book")
//                .addAggregation(builder)
//                .setSize(4).get();//每次查询三个
//        Max max  = response.getAggregations().get("weightMax");
//        System.out.println(max.getValue());
        //求最小值
//        AggregationBuilder builder = AggregationBuilders.min("weightMin").field("weight");
//        SearchResponse response = client.prepareSearch("book")
//                .addAggregation(builder)
//                .setSize(4).get();//每次查询三个
//        Min min  = response.getAggregations().get("weightMin");
//        System.out.println(min.getValue());
        //求平均值
//        AggregationBuilder builder = AggregationBuilders.avg("weightAvg").field("weight");
//        SearchResponse response = client.prepareSearch("book")
//                .addAggregation(builder)
//                .setSize(4).get();//每次查询三个
//        Avg avg  = response.getAggregations().get("weightAvg");
//        System.out.println(avg.getValue());
        //求总和
//        AggregationBuilder builder = AggregationBuilders.sum("weightSum").field("weight");
//        SearchResponse response = client.prepareSearch("book")
//                .addAggregation(builder)
//                .setSize(4).get();//每次查询三个
//        Sum sum  = response.getAggregations().get("weightSum");
//        System.out.println(sum.getValue());
        //求基数(互不相同的值的个数)
//        AggregationBuilder builder = AggregationBuilders.cardinality("weightCardinality").field("weight");
//        SearchResponse response = client.prepareSearch("book")
//                .addAggregation(builder)
//                .setSize(4).get();//每次查询三个
//        Cardinality cardinality  = response.getAggregations().get("weightCardinality");
//        System.out.println(cardinality.getValue());
        //求分组
//        AggregationBuilder builder = AggregationBuilders.terms("terms").field("weight");
//        SearchResponse response = client.prepareSearch("book")
//                .addAggregation(builder)
//                .setSize(4).get();//每次查询三个
//        Terms terms  = response.getAggregations().get("terms");
//        terms.getBuckets().forEach(t-> System.out.println(t.getKey()+":"+t.getDocCount()));
        //求filter
//        QueryBuilder queryBuilder = QueryBuilders.termQuery("weight","2");
//        AggregationBuilder builder = AggregationBuilders.filter("filter",queryBuilder);
//        SearchResponse response = client.prepareSearch("book")
//                .addAggregation(builder)
//                .setSize(4).get();//每次查询三个
//        Filter filter  = response.getAggregations().get("filter");
//        System.out.println(filter.getDocCount());
        //filters聚合
        try{
            AggregationBuilder builder = AggregationBuilders
                    .filters("filters",
                            new FiltersAggregator.KeyedFilter("ymlakes",QueryBuilders.termQuery("user","ymlakes")),
                            new FiltersAggregator.KeyedFilter("kuluoluo",QueryBuilders.termQuery("user","kuluoluo")));
            SearchResponse response = client.prepareSearch("accounts")
                    .addAggregation(builder)
                    .setSize(4).get();//每次查询三个
            Filters filters  = response.getAggregations().get("filters");
            filters.getBuckets().forEach(t-> System.out.println(t.getKey()+":"+t.getDocCount()));
        }catch (Exception e){

        }
        //missing聚合（统计某个字段是null）
        //range聚合

    }

    @Test
    public void testQueryString(){
        //QueryBuilder queryBuilder = QueryBuilders.commonTermsQuery("user","kuluoluo");
        //两个条件都满足
        //QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("+kuluoluo -ymlakes");
        //满足其中一个即可
        QueryBuilder queryBuilder = QueryBuilders.simpleQueryStringQuery("+kuluoluo -ymlakes");
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(queryBuilder)
                .setSize(4).get();//每次查询三个
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    //组合查询
    @Test
    public void testBoolQuery(){
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("user","ymlakes"))
                .mustNot(QueryBuilders.matchQuery("title","python"))
                .should(QueryBuilders.matchQuery("title","python"));
               // .filter(QueryBuilders.rangeQuery("").gt(""));
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(queryBuilder)
                .setSize(4).get();//每次查询三个
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    @Test
    public void testHighLight(){
        QueryBuilder matchQuery = QueryBuilders.matchQuery("title", "php");
        HighlightBuilder hiBuilder=new HighlightBuilder();
        hiBuilder.preTags("<h2>");
        hiBuilder.postTags("</h2>");
        hiBuilder.field("title");
        // 搜索数据
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(matchQuery)
                .highlighter(hiBuilder)
                .execute().actionGet();
        for (SearchHit searchHit : response.getHits()) {
            System.out.println(searchHit.getHighlightFields());
        }
    }

    @Test
    public void testSort(){
        SearchResponse response = client.prepareSearch("book")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort("weight", SortOrder.DESC)
                .execute().actionGet();
        for (SearchHit searchHit : response.getHits()) {
            System.out.println(searchHit.getHighlightFields()+":"+searchHit.getSourceAsString());
        }
    }

    //分页
    @Test
    public void testPaging(){
        SearchResponse response = client.prepareSearch("accounts")
                .setQuery(QueryBuilders.matchAllQuery())
                .setFrom(5)//从第几条开始查找
                .setSize(2)//每次查两条
                .execute().actionGet();
        for (SearchHit searchHit : response.getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

    }




}
