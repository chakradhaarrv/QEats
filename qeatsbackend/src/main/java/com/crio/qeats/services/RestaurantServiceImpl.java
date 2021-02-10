
/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.services;

import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.exchanges.GetRestaurantsRequest;
import com.crio.qeats.exchanges.GetRestaurantsResponse;
import com.crio.qeats.repositoryservices.RestaurantRepositoryService;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class RestaurantServiceImpl implements RestaurantService {

  private final Double peakHoursServingRadiusInKms = 3.0;
  private final Double normalHoursServingRadiusInKms = 5.0;
  @Autowired
  private RestaurantRepositoryService restaurantRepositoryService;


  // TODO: CRIO_TASK_MODULE_RESTAURANTSAPI - Implement findAllRestaurantsCloseby.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findAllRestaurantsCloseBy(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
    
    Double latitude = getRestaurantsRequest.getLatitude();
    Double longitude = getRestaurantsRequest.getLongitude();
    Double servingRadiusInKms = -1.0;

    LocalTime peakOneStart = LocalTime.of(7, 59, 59);
    LocalTime peakOneEnd = LocalTime.of(10, 00, 01);
    LocalTime peakTwoStart = LocalTime.of(12, 59, 59);
    LocalTime peakTwoEnd = LocalTime.of(14, 00, 01);
    LocalTime peakThreeStart = LocalTime.of(18, 59, 59);
    LocalTime peakThreeEnd = LocalTime.of(21, 00, 01);

    if (currentTime.isBefore(peakOneEnd) && currentTime.isAfter(peakOneStart)) {
      servingRadiusInKms = 3.0;
    } else if (currentTime.isBefore(peakTwoEnd) && currentTime.isAfter(peakTwoStart)) {
      servingRadiusInKms = 3.0;
    } else if (currentTime.isBefore(peakThreeEnd) && currentTime.isAfter(peakThreeStart)) {
      servingRadiusInKms = 3.0;
    } else {
      servingRadiusInKms = 5.0;
    }

    List<Restaurant> restaurantsCloseBy = restaurantRepositoryService.findAllRestaurantsCloseBy(
        latitude, longitude, currentTime, servingRadiusInKms);
    return new GetRestaurantsResponse(restaurantsCloseBy);
  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Implement findRestaurantsBySearchQuery. The request object has the search string.
  // We have to combine results from multiple sources:
  // 1. Restaurants by name (exact and inexact)
  // 2. Restaurants by cuisines (also called attributes)
  // 3. Restaurants by food items it serves
  // 4. Restaurants by food item attributes (spicy, sweet, etc)
  // Remember, a restaurant must be present only once in the resulting list.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQuery(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {

    Double latitude = getRestaurantsRequest.getLatitude();
    Double longitude = getRestaurantsRequest.getLongitude();
    Double servingRadiusInKms = -1.0;
    String searchString = getRestaurantsRequest.getSearchFor();

    LocalTime peakOneStart = LocalTime.of(7, 59, 59);
    LocalTime peakOneEnd = LocalTime.of(10, 00, 01);
    LocalTime peakTwoStart = LocalTime.of(12, 59, 59);
    LocalTime peakTwoEnd = LocalTime.of(14, 00, 01);
    LocalTime peakThreeStart = LocalTime.of(18, 59, 59);
    LocalTime peakThreeEnd = LocalTime.of(21, 00, 01);

    if (currentTime.isBefore(peakOneEnd) && currentTime.isAfter(peakOneStart)) {
      servingRadiusInKms = 3.0;
    } else if (currentTime.isBefore(peakTwoEnd) && currentTime.isAfter(peakTwoStart)) {
      servingRadiusInKms = 3.0;
    } else if (currentTime.isBefore(peakThreeEnd) && currentTime.isAfter(peakThreeStart)) {
      servingRadiusInKms = 3.0;
    } else {
      servingRadiusInKms = 5.0;
    }

    List<Restaurant> restaurantsCloseBy = new ArrayList<>();
    
    if (searchString != null
        && !searchString.isEmpty()) {
      
      restaurantsCloseBy = restaurantRepositoryService.findRestaurantsByName(
        latitude, longitude,
        searchString, currentTime, servingRadiusInKms);
      
      restaurantsCloseBy.addAll(restaurantRepositoryService.findRestaurantsByAttributes(
          latitude, longitude, searchString, currentTime, servingRadiusInKms));
      
      restaurantsCloseBy.addAll(restaurantRepositoryService.findRestaurantsByItemName(
          latitude, longitude, searchString, currentTime, servingRadiusInKms));

      restaurantsCloseBy.addAll(restaurantRepositoryService.findRestaurantsByNameExact(
          latitude, longitude, searchString, currentTime, servingRadiusInKms));
      
      restaurantsCloseBy.addAll(restaurantRepositoryService.findRestaurantsByItemAttributes(
          latitude, longitude, searchString, currentTime, servingRadiusInKms));

      return new GetRestaurantsResponse(restaurantsCloseBy);
    }
    return new GetRestaurantsResponse(restaurantsCloseBy);
  }

}

