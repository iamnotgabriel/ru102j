package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanResult;

import java.util.*;

public class SiteDaoRedisImpl implements SiteDao {
    private final JedisPool jedisPool;

    public SiteDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // When we insert a site, we set all of its values into a single hash.
    // We then store the site's id in a set for easy access.
    @Override
    public void insert(Site site) {
        try (Jedis jedis = jedisPool.getResource()) {
            String hashKey = RedisSchema.getSiteHashKey(site.getId());
            String siteIdKey = RedisSchema.getSiteIDsKey();
            jedis.hmset(hashKey, site.toMap());
            jedis.sadd(siteIdKey, hashKey);
        }
    }

    @Override
    public Site findById(long id) {
        try(Jedis jedis = jedisPool.getResource()) {
            String key = RedisSchema.getSiteHashKey(id);
            Map<String, String> fields = jedis.hgetAll(key);
            if (fields == null || fields.isEmpty()) {
                return null;
            } else {
                return new Site(fields);
            }
        }
    }



    // Challenge #1
    @Override
    public Set<Site> findAll() {
        try (Jedis jedis = jedisPool.getResource()) {
            String siteIdsKey = RedisSchema.getSiteIDsKey();
            Set<Site> sites = new HashSet<>();
            String cursor = "0";
            do {
                ScanResult<String> siteIds = jedis.sscan(siteIdsKey, cursor);
                cursor = siteIds.getCursor();
                sites.addAll(getSites(jedis, siteIds.getResult()));
            } while(!cursor.equals("0"));

            return sites;
        }
    }

    private Set<Site> getSites(Jedis jedis, List<String> siteIds) {
        Set<Site> sites = new HashSet<>();
        for (String sideId : siteIds) {
            Map<String, String> fields = jedis.hgetAll(sideId);
            Site site = toSite(fields);
            if (site != null) {
                sites.add(site);
            }
        }
        return sites;
    }

    private Site toSite(Map<String, String> fields) {
        if (fields == null || fields.isEmpty()) {
            return null;
        }
        return new Site(fields);
    }
}
