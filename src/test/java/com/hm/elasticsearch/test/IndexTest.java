package com.hm.elasticsearch.test;

import com.hm.elasticsearch.ElasticsearchApplication;
import com.hm.elasticsearch.dao.ItemRepository;
import com.hm.elasticsearch.pojo.Item;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.lang.annotation.Native;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangzhijun on 2019/4/3.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ElasticsearchApplication.class)
public class IndexTest {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Autowired
    private ItemRepository itemRepository;

    /**
     *创建索引
     */
    @Test
    public void testCreate(){
        //创建索引，会根据Item类的@Document主键信息来创建
        elasticsearchTemplate.createIndex(Item.class);
        //配置映射，会根据Item类中的ID，Field等自动来自动完成映射
        elasticsearchTemplate.putMapping(Item.class);
    }

    /**
     *删除索引
     */
    @Test
    public void deleteIndex(){
        //创建索引，会根据Item类的@Document主键信息来创建
        elasticsearchTemplate.deleteIndex("hm");
    }

    @Test
    public  void adddata(){
        Item item=new Item();
        item.setId(1L);
        item.setBrand("小米2");
        item.setCategory("手机2");
        item.setImages("http://www.baidu.com");
        item.setPrice(2000d);
        item.setTitle("小米手机8");
        itemRepository.save(item);
    }

    @Test
    public void batchAdd(){
        List<Item> list=new ArrayList<>();
        Item item=new Item();
        item.setId(1L);
        item.setBrand("小米");
        item.setCategory("手机");
        item.setImages("http://www.baidu.com");
        item.setPrice(2000d);
        item.setTitle("小米手机");
        list.add(item);

        Item item1=new Item();
        item1.setId(2L);
        item1.setBrand("小米");
        item1.setCategory("手机");
        item1.setImages("http://www.baidu.com");
        item1.setPrice(3000d);
        item1.setTitle("小米手机");
        list.add(item1);

        Item item2=new Item();
        item2.setId(3L);
        item2.setBrand("小米3");
        item2.setCategory("手机");
        item2.setImages("http://www.baidu.com");
        item2.setPrice(2000d);
        item2.setTitle("小米手机3");
        list.add(item2);

     itemRepository.saveAll(list);
    }


    /**
     * 查询所有数据
     */
    @Test
    public void findFind(){
        //查询全部，并按照价格降序排序
        Iterable<Item> price = itemRepository.findAll(Sort.by(Sort.Direction.DESC, "price"));
        price.forEach(item -> {
            System.out.println(item);
        });
    }

    /**
     * 自定义方法查询
     */
    @Test
    public void findBypriceBetween(){
        List<Item> byPriceBetween = itemRepository.findByPriceBetween(2000, 3000);

        byPriceBetween.forEach(item -> {
            System.out.println(item);
        });
    }

    /**
     * 基本查询
     */
    @Test
    public void testQuery(){
        //   MatchQueryBuilder queryBuilder1 = QueryBuilders.matchQuery("title", "小米");
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("小米3", "title", "brand");
        Iterable<Item> search = itemRepository.search(multiMatchQueryBuilder);
        search.forEach(item -> {
            System.out.println(item);
        });
    }


    /**
     * 自定义查询 测试分页，排序，自定义条件
     */
    @Test
    public void testNativeQuery(){
        //构建查询条件
        NativeSearchQueryBuilder queryBuilder=new NativeSearchQueryBuilder();
        //添加基本的分词查询
        queryBuilder.withQuery(QueryBuilders.multiMatchQuery("小米3","title","brand"));
        //设置分页参数
        queryBuilder.withPageable(PageRequest.of(0,3));
        //根据价格排序
        queryBuilder.withSort(SortBuilders.scoreSort().order(SortOrder.DESC)).withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC)).withSort(SortBuilders.fieldSort("id").order(SortOrder.ASC));
        //执行搜索获取结果
        Page<Item> search = this.itemRepository.search(queryBuilder.build());
        //打印总条数
        System.out.println(search.getTotalElements());
        //打印总页数
        System.out.println(search.getTotalPages());
        //打印当前页
        System.out.println(search.getNumber());
        //打印页数大小
        System.out.println(search.getSize());
        //打印结果集
        search.forEach(item -> {
            System.out.println(item);
        });
    }

    @Test
    public void  testAgg(){
        //构建查询条件
        NativeSearchQueryBuilder searchQueryBuilder=new NativeSearchQueryBuilder();
        //不查询任何结果
        searchQueryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""}, null));
        //添加一个新的聚合，聚合类型为terms,聚合名称为brands,聚合字段为brand
        searchQueryBuilder.addAggregation(AggregationBuilders.terms("brands").field("brand"));
        //查询，需要把结果强转为AggregatedPage类型
        AggregatedPage<Item> aggPage = (AggregatedPage<Item>)this.itemRepository.search(searchQueryBuilder.build());
        System.out.println("aggPage:"+aggPage);
        //3.解析
        //从结果中取出名为brands的那个聚合
        //因为是利用String类型字段来进行的term聚合，所以结果要强转为StringTerm类型
        StringTerms agg = (StringTerms) aggPage.getAggregation("brands");
        System.out.println("agg"+agg);
        //获取桶
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        //遍历
        buckets.forEach(item->{
            //获取桶的key
            System.out.println(item.getKeyAsString());
            //获取桶的文档数量
            System.out.println(item.getDocCount());
        });
    }




}
