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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.cascades.CascadesRuleCall;
import org.apache.calcite.plan.cascades.CascadesTestUtils;
import org.apache.calcite.plan.cascades.ImplementationRule;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;

/**
 *
 */
public class CascadesTestFilterRule extends ImplementationRule<LogicalFilter> {
  public static final CascadesTestFilterRule CASCADES_FILTER_RULE =
      new CascadesTestFilterRule();

  public CascadesTestFilterRule() {
    super(LogicalFilter.class,
        RelOptUtil::notContainsWindowedAgg,
        Convention.NONE, CascadesTestUtils.CASCADES_TEST_CONVENTION,
        RelFactories.LOGICAL_BUILDER, "CascadesFilterRule");
  }

  @Override public void implement(LogicalFilter rel, RelTraitSet requestedTraits,
      CascadesRuleCall call) {
    RelNode input = rel.getInput();
    // Pull distribution from child
    requestedTraits = requestedTraits.plus(RelDistributionTraitDef.INSTANCE.getDefault());
    input = convert(input, requestedTraits);
    CascadesTestFilter filter =
        new CascadesTestFilter(rel.getCluster(), requestedTraits, input, rel.getCondition());
    call.transformTo(filter);
  }
}