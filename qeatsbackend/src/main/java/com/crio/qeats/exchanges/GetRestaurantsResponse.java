/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.exchanges;

import com.crio.qeats.dto.Restaurant;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GetRestaurantsResponse {

  List<Restaurant> restaurants;

  public void setRestaurants(List<Restaurant> restaurants) {
    this.restaurants = restaurants;
  }

  public List<Restaurant> getRestaurants() {
    return restaurants;
  }

}

// }
