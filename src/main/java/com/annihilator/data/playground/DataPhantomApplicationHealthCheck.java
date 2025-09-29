package com.annihilator.data.playground;

import com.codahale.metrics.health.HealthCheck;

public class DataPhantomApplicationHealthCheck extends HealthCheck {

  @Override
  protected Result check() {

    return Result.healthy();
  }
}
