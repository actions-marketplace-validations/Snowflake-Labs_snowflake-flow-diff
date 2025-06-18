package com.snowflake.openflow.checkstyle;

import org.apache.nifi.registry.flow.FlowSnapshotContainer;

import java.util.List;

public interface CheckstyleRule {

    String id();

    List<String> check(final FlowSnapshotContainer container, final String flowName, final CheckstyleRulesConfig.RuleConfig config);

    boolean ruleApplies(final List<String> includes, final List<String> excludes, final CheckstyleRulesConfig config, final String flowName);

}
