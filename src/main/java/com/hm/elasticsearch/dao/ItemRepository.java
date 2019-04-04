package com.hm.elasticsearch.dao;

import com.hm.elasticsearch.pojo.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by wangzhijun on 2019/4/3.
 */
public interface ItemRepository extends ElasticsearchRepository<Item,Long>{

    public List<Item> findByPriceBetween(double pricel,double price2);
}
