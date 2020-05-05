/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.plan.cascades.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.cascades.CascadesTestUtils;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.TraitsEnforcementRule;
import org.apache.calcite.rel.logical.LogicalExchange;

/**
 *
 */
public class CascadesTestExchangeRule extends TraitsEnforcementRule {
  public static final CascadesTestExchangeRule CASCADES_EXCHANGE_RULE =
      new CascadesTestExchangeRule();

  public CascadesTestExchangeRule() {
    super(LogicalExchange.class, Convention.NONE, CascadesTestUtils.CASCADES_TEST_CONVENTION,
        "CascadesExchangeRule");
  }

  @Override public RelNode convert(RelNode rel) {
    LogicalExchange exchange = (LogicalExchange)rel;
     // Exchange preserves any other trait except distribution. We may request RelDistribution.ANY.
    RelTraitSet requestedTraits = rel.getTraitSet()
        .plus(RelDistributions.ANY)
        .plus(CascadesTestUtils.CASCADES_TEST_CONVENTION);
    CascadesTestExchange newExchange =  CascadesTestExchange.create(
        convert(exchange.getInput(), requestedTraits),
        exchange.getDistribution()
    );
    return newExchange;
  }
}
