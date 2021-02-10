/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.repositoryservices;

import ch.hsr.geohash.GeoHash;
import com.crio.qeats.configs.RedisConfiguration;
import com.crio.qeats.dto.Item;
import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.globals.GlobalConstants;
import com.crio.qeats.models.ItemEntity;
import com.crio.qeats.models.MenuEntity;
import com.crio.qeats.models.RestaurantEntity;
import com.crio.qeats.repositories.ItemRepository;
import com.crio.qeats.repositories.MenuRepository;
import com.crio.qeats.repositories.RestaurantRepository;
import com.crio.qeats.utils.GeoLocation;
import com.crio.qeats.utils.GeoUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;


@Service
public class RestaurantRepositoryServiceImpl implements RestaurantRepositoryService {


  @Autowired
  private RestaurantRepository restaurantRepository;

  @Autowired
  private MenuRepository menuRepository;

  @Autowired
  private RedisConfiguration redisConfiguration;

  @Autowired
  private MongoTemplate mongoTemplate;

  @Autowired
  private Provider<ModelMapper> modelMapperProvider;

  private boolean isOpenNow(LocalTime time, RestaurantEntity res) {
    LocalTime openingTime = LocalTime.parse(res.getOpensAt());
    LocalTime closingTime = LocalTime.parse(res.getClosesAt());

    return time.isAfter(openingTime) && time.isBefore(closingTime);
  }

  // TODO: CRIO_TASK_MODULE_NOSQL
  // Objectives:
  // 1. Implement findAllRestaurantsCloseby.
  // 2. Remember to keep the precision of GeoHash in mind while using it as a key.
  // Check RestaurantRepositoryService.java file for the interface contract.
  public List<Restaurant> findAllRestaurantsCloseBy(Double latitude,
      Double longitude, LocalTime currentTime, Double servingRadiusInKms) {

    List<Restaurant> restaurants = new ArrayList<>();
    if (!redisConfiguration.isCacheAvailable()) {
      redisConfiguration.initCache();
    }
    Jedis jedis = redisConfiguration.getJedisPool().getResource();
    GeoHash geoHash = GeoHash.withCharacterPrecision(latitude, longitude, 7);
    String restaurantJson = jedis.get(geoHash.toBase32());
    List<RestaurantEntity> restaurantEntityList = new ArrayList<>();
    ObjectMapper objectMapper = new ObjectMapper();
    if (redisConfiguration.isCacheAvailable() && restaurantJson != null) {
      try {
        RestaurantEntity[] restEntArr = objectMapper.readValue(restaurantJson,
         RestaurantEntity[].class);
        restaurantEntityList = Arrays.asList(restEntArr);
      } catch (IOException | NullPointerException e) {
        e.printStackTrace();
      }
    } else {
      try {
        restaurantEntityList = restaurantRepository.findAll();
        jedis.setex(geoHash.toBase32(), 3600, objectMapper.writeValueAsString(
            restaurantEntityList));
      } catch (JsonProcessingException j) {
        j.printStackTrace();
      }
    }
    for (RestaurantEntity restaurantEntity : restaurantEntityList) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime,
            latitude, longitude, servingRadiusInKms)) {
        ModelMapper mapper = modelMapperProvider.get();
        Restaurant restaurant2 = mapper.map(restaurantEntity, Restaurant.class);
        restaurants.add(restaurant2);
      }
    }
    // TODO: CRIO_TASK_MODULE_REDIS
    // We want to use cache to speed things up. Write methods that perform the same functionality,
    // but using the cache if it is present and reachable.
    // Remember, you must ensure that if cache is not present, the queries are directed at the
    // database instead.


    //CHECKSTYLE:OFF
    //CHECKSTYLE:ON
    return restaurants;
  }

  // TODO: CRIO_TASK_MODULE_NOSQL
  // Objective:
  // 1. Check if a restaurant is nearby and open. If so, it is a candidate to be returned.
  // NOTE: How far exactly is "nearby"?


  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose names have an exact or partial match with the search query.
  @Override
  public List<Restaurant> findRestaurantsByName(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    
    List<Restaurant> restaurantList = new ArrayList<>();
    if (searchString == null || searchString.isEmpty()) {
      return restaurantList;
    }

    List<Restaurant> allrest = findAllRestaurantsCloseBy(latitude,
        longitude, currentTime, servingRadiusInKms);

    for (int i = 0; i < allrest.size(); i++) {
      if (allrest.get(i).getName().equals(searchString)) {
        restaurantList.add(allrest.get(i));
      }
    }

    for (int i = 0; i < allrest.size(); i++) {
      if (allrest.get(i).getName().contains(searchString)) {
        if (!restaurantList.contains(allrest.get(i))) {
          restaurantList.add(allrest.get(i));
        }
      }
    }

    return restaurantList;
  }

  @Override
  public List<Restaurant> findRestaurantsByNameExact(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    
    List<Restaurant> restaurantList = new ArrayList<>();
    if (searchString == null || searchString.isEmpty()) {
      return restaurantList;
    }

    List<Restaurant> allrest = findAllRestaurantsCloseBy(latitude,
        longitude, currentTime, servingRadiusInKms);

    for (int i = 0; i < allrest.size(); i++) {
      if (allrest.get(i).getName().equals(searchString)) {
        restaurantList.add(allrest.get(i));
      }
    }

    return restaurantList;
  }


  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose attributes (cuisines) intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByAttributes(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    
    List<Restaurant> restaurantList = new ArrayList<>();
    if (searchString == null) {
      return restaurantList;
    }

    List<Restaurant> allrest = findAllRestaurantsCloseBy(latitude,
        longitude, currentTime, servingRadiusInKms);

    for (int i = 0; i < allrest.size(); i++) {
      if (allrest.get(i).getAttributes().contains(searchString)) {
        restaurantList.add(allrest.get(i));
      }
    }

    for (int i = 0; i < allrest.size(); i++) {
      List<String> att = allrest.get(i).getAttributes();
      for (int j = 0; j < att.size(); j++) {
        if (att.get(j).contains(searchString)) {
          if (!restaurantList.contains(allrest.get(i))) {
            restaurantList.add(allrest.get(i));
            break;
          }
        }
      }
    }
    return restaurantList;
  }



  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose names form a complete or partial match
  // with the search query.

  @Override
  public List<Restaurant> findRestaurantsByItemName(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {


    List<Restaurant> restaurantList = new ArrayList<>();
    if (searchString == null) {
      return restaurantList;
    }

    List<Restaurant> closerest = findAllRestaurantsCloseBy(latitude,
        longitude, currentTime, servingRadiusInKms);

    List<RestaurantEntity> allrest = restaurantRepository.findAll();


    List<MenuEntity> menus = menuRepository.findAll();
    //exact match
    for (int i = 0; i < menus.size(); i++) {
      List<Item> items = menus.get(i).getItems();
      for (int j = 0; j < items.size(); j++) {
        if (items.get(j).getName().equals(searchString)) {
          RestaurantEntity temp = new RestaurantEntity();
          for (int k = 0; k < allrest.size(); k++) {
            if (allrest.get(k).getRestaurantId().equals(menus.get(i).getRestaurantId())) {
              temp = allrest.get(j);
              break;
            }
          }

          if (closerest.contains(changeto(temp)) && !allrest.contains(temp)) {
            restaurantList.add(changeto(temp));
          }

        }
      }
    }
    //partial match
    for (int i = 0; i < menus.size(); i++) {
      List<Item> items = menus.get(i).getItems();
      for (int j = 0; j < items.size(); j++) {
        if (items.get(j).getName().contains(searchString)) {
          RestaurantEntity temp = new RestaurantEntity();
          for (int k = 0; k < allrest.size(); k++) {
            if (allrest.get(k).getRestaurantId().equals(menus.get(i).getRestaurantId())) {
              temp = allrest.get(j);
              break;
            }
          }

          if (closerest.contains(changeto(temp)) && !allrest.contains(temp)) {
            restaurantList.add(changeto(temp));
          }

        }
      }

    }

    return restaurantList;
  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose attributes intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByItemAttributes(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {

    List<Restaurant> restaurantList = new ArrayList<>();
    if (searchString == null || searchString.isEmpty()) {
      return restaurantList;
    }

    List<Restaurant> closerest = findAllRestaurantsCloseBy(latitude,
        longitude, currentTime, servingRadiusInKms);

    List<RestaurantEntity> allrest = restaurantRepository.findAll();


    List<MenuEntity> menus = menuRepository.findAll();
    //exact match
    for (int i = 0; i < menus.size(); i++) {
      List<Item> items = menus.get(i).getItems();
      for (int j = 0; j < items.size(); j++) {
        if (items.get(j).getAttributes().contains(searchString)) {
          RestaurantEntity temp = new RestaurantEntity();
          for (int k = 0; k < allrest.size(); k++) {
            if (allrest.get(k).getRestaurantId().equals(menus.get(i).getRestaurantId())) {
              temp = allrest.get(j);
              break;
            }
          }
          if (closerest.contains(changeto(temp)) && !allrest.contains(temp)) {
            restaurantList.add(changeto(temp));
          }
        }
      }
    }
    //partial match
    for (int i = 0; i < menus.size(); i++) {
      List<Item> items = menus.get(i).getItems();
      for (int j = 0; j < items.size(); j++) {
        List<String> atts = items.get(j).getAttributes();
        for (int l = 0; l < atts.size(); l++) {
          if (atts.get(j).contains(searchString)) {
            RestaurantEntity temp = new RestaurantEntity();
            for (int k = 0; k < allrest.size(); k++) {
              if (allrest.get(k).getRestaurantId().equals(menus.get(i).getRestaurantId())) {
                temp = allrest.get(j);
                break;
              }
            }
            if (closerest.contains(changeto(temp)) && !allrest.contains(temp)) {
              restaurantList.add(changeto(temp));
            }

          }
        }
      }
    }
    return restaurantList;
  }





  /**
   * Utility method to check if a restaurant is within the serving radius at a given time.
   * @return boolean True if restaurant falls within serving radius and is open, false otherwise
   */
  private boolean isRestaurantCloseByAndOpen(RestaurantEntity restaurantEntity,
      LocalTime currentTime, Double latitude, Double longitude, Double servingRadiusInKms) {
    if (isOpenNow(currentTime, restaurantEntity)) {
      return GeoUtils.findDistanceInKm(latitude, longitude,
          restaurantEntity.getLatitude(), restaurantEntity.getLongitude())
          < servingRadiusInKms;
    }

    return false;
  }


  private Restaurant changeto(RestaurantEntity restaurant) {
    Restaurant temp = new Restaurant();
    temp.setAttributes(restaurant.getAttributes());
    temp.setId(restaurant.getId());
    temp.setRestaurantId(restaurant.getRestaurantId());
    temp.setName(restaurant.getName());
    temp.setCity(restaurant.getCity());
    temp.setImageUrl(restaurant.getImageUrl());
    temp.setLatitude(restaurant.getLatitude());
    temp.setLongitude(restaurant.getLongitude());
    temp.setOpensAt(restaurant.getOpensAt());
    temp.setClosesAt(restaurant.getClosesAt());

    return temp;
  }


}

