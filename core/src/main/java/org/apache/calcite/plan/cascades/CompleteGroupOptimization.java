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
package org.apache.calcite.plan.cascades;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.convert.TraitsEnforcementRule;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.calcite.plan.cascades.CascadesUtils.difference;
import static org.apache.calcite.plan.cascades.CascadesUtils.isLogical;

/**
 * Planner task that performs a finalization of the group optimization for the given traits:
 * marks group optimized for context and apply traits enforcers.
 * TODO: Remove it?
 */
public class CompleteGroupOptimization extends CascadesTask  {
  private final RelGroup group;

  CompleteGroupOptimization(CascadesTask parentTask, RelGroup group, double newUpperBound,
      RelTraitSet requestedTraits) {
    super(parentTask, newUpperBound, requestedTraits);
    this.group = group;
  }

  @Override void perform() {
    group.markOptimized(requestedTraits);
    for (RelNode relNode : new ArrayList<>(group.physicalRels())) {
      if (isLogical(relNode)) {
        continue;
      }

      RelTraitSet diff = difference(relNode.getTraitSet(), requestedTraits);

      outer :
      for (RelTrait trait : diff) { // TODO generate all combinations of enforcers
        // TODO Handle composite traits more elegantly
//        List<RelTrait> tList = diff.getTraits(trait.getTraitDef());
//        for (RelTrait t : tList) {
          relNode = enforce(relNode, trait);
          if (relNode == null) {
            break outer;
          }
//        }
      }
    }
    RelSubGroup subGroup = group.getSubGroup(requestedTraits);
    if (subGroup.winnerRel() == null) {
      for (RelNode physicalNode : group.physicalRels()) {
        subGroup.updateWinnerIfNeeded(physicalNode);
      }
    }
  }

  private <T extends RelTrait> RelSubGroup enforce(RelNode rel, T toTrait) {
    RelTraitDef<T> traitDef = toTrait.getTraitDef();
    T fromTrait = rel.getTraitSet().getTrait(traitDef); // TODO simplify?

    if (fromTrait.satisfies(toTrait)) {
      return planner.getSubGroup(rel);
    }
    // TODO composite traits handling
    if (!traitDef.canConvert(planner, fromTrait, toTrait, rel)) {
      return null;
    }

    RelNode convertedRel = traitDef.convert(planner, rel, toTrait, false);

    if (!isLogical(convertedRel)) {
      return planner.register(convertedRel, rel);
    } else {
      Collection<TraitsEnforcementRule> converters = planner.getConverters(convertedRel);
      ConverterRule converter = converters == null || converters.isEmpty()
          ? null
          : converters.iterator().next();

      if (converter == null) {
        return null;
      }
      RelNode enforcedRel = converter.convert(convertedRel);
      if (enforcedRel == null) {
        return null;
      }
      return planner.register(enforcedRel, rel);
    }
  }

  @Override protected void description(StringBuilder stringBuilder) {
    stringBuilder
        .append("CompleteGroupOptimization{group=")
        .append(group)
        .append(", traits=")
        .append(requestedTraits)
        .append('}');
  }
}