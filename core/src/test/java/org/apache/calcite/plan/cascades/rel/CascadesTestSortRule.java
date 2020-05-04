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
import org.apache.calcite.plan.cascades.RelSubGroup;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalSort;

/**
 *
 */
public class CascadesTestSortRule extends ConverterRule {
  public static final CascadesTestSortRule CASCADES_SORT_RULE =
      new CascadesTestSortRule();

  public CascadesTestSortRule() {
    super(LogicalSort.class, Convention.NONE, CascadesTestUtils.CASCADES_TEST_CONVENTION,
        "CascadesSortRule");
  }

  @Override public RelNode convert(RelNode rel) {
    LogicalSort sort = (LogicalSort)rel;
    // Sort preserves any other trait except sorting. We may request RelCollation.ANY.
    RelTraitSet requestedTraits = sort.getTraitSet()
        .plus(RelCollationTraitDef.INSTANCE.getDefault())
        .plus(CascadesTestUtils.CASCADES_TEST_CONVENTION);

    RelNode input = convert(sort.getInput(), requestedTraits);

    CascadesTestSort newSort = CascadesTestSort.create(
        input,
        sort.getCollation(),
        null,
        null);

    return newSort;
  }
}
