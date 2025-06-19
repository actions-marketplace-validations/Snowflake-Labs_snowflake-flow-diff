package com.snowflake.openflow.checkstyle;

import com.snowflake.openflow.checkstyle.CheckstyleRulesConfig.RuleConfig;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.parameter.ExpressionLanguageAgnosticParameterParser;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public enum DefaultCheckstyleRules implements CheckstyleRule {

    CONCURRENT_TASKS("concurrentTasks") {
        @Override
        public List<String> check(final FlowSnapshotContainer container, final String flowName, final RuleConfig config) {
            final VersionedProcessGroup rootProcessGroup = container.getFlowSnapshot().getFlowContents();
            return checkConcurrentTasks(rootProcessGroup, config, flowName);
        }

        private List<String> checkConcurrentTasks(final VersionedProcessGroup processGroup, final RuleConfig ruleConfig, final String flowName) {
            final List<String> violations = new ArrayList<>();

            for (final VersionedProcessGroup childGroup : processGroup.getProcessGroups()) {
                violations.addAll(checkConcurrentTasks(childGroup, ruleConfig, flowName));
            }

            int limit = 2;

            if (ruleConfig != null && ruleConfig.parameters() != null && ruleConfig.parameters().get("limit") != null) {
                limit = ((Number) ruleConfig.parameters().get("limit")).intValue();
            }

            if (ruleConfig != null && ruleConfig.overrides() != null) {
                for (final Map.Entry<String, Map<String, Object>> entry : ruleConfig.overrides().entrySet()) {
                    if (flowName != null && flowName.matches(entry.getKey())) {
                        final Object val = entry.getValue().get("limit");
                        if (val != null) {
                            limit = ((Number) val).intValue();
                        }
                    }
                }
            }

            for (final VersionedProcessor processor : processGroup.getProcessors()) {
                final int concurrentTasks = processor.getConcurrentlySchedulableTaskCount();
                if (concurrentTasks > limit) {
                    violations.add("Processor named `" + processor.getName() + "` is configured with " + concurrentTasks + " concurrent tasks");
                }
            }

            return violations;
        }
    },

    SNAPSHOT_METADATA("snapshotMetadata") {
        @Override
        public List<String> check(final FlowSnapshotContainer container, final String flowName, final RuleConfig config) {
            final List<String> violations = new ArrayList<>();
            if (container.getFlowSnapshot().getSnapshotMetadata() == null) {
                violations.add("Flow snapshot metadata is missing");
            }
            return violations;
        }
    },

    EMPTY_PARAMETER("emptyParameter") {
        @Override
        public List<String> check(final FlowSnapshotContainer container, final String flowName, final RuleConfig config) {
            final List<String> violations = new ArrayList<>();
            final List<VersionedParameter> parameters = container.getFlowSnapshot()
                    .getParameterContexts()
                    .values()
                    .stream()
                    .flatMap(context -> context.getParameters().stream())
                    .toList();

            for (VersionedParameter parameter : parameters) {
                if (parameter.getValue() != null && parameter.getValue().isEmpty()) {
                    violations.add("Parameter named `" + parameter.getName() + "` is set to empty string");
                }
            }

            return violations;
        }
    },

    DEFAULT_PARAMETERS("defaultParameters") {
        @Override
        public List<String> check(final FlowSnapshotContainer container, final String flowName, final RuleConfig config) {
            final List<String> violations = new ArrayList<>();
            final List<VersionedParameter> parameters = container.getFlowSnapshot()
                    .getParameterContexts()
                    .values()
                    .stream()
                    .flatMap(context -> context.getParameters().stream())
                    .toList();

            List<String> parameterNamesWithDefaultValue = new ArrayList<String>();

            if (config != null && config.parameters() != null && config.parameters().get("defaultParameters") != null) {
                final String input = config.parameters().get("defaultParameters").toString();
                parameterNamesWithDefaultValue = Arrays.stream(input.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
            }

            if (config != null && config.overrides() != null) {
                for (final Map.Entry<String, Map<String, Object>> entry : config.overrides().entrySet()) {
                    if (flowName != null && flowName.matches(entry.getKey())) {
                        final Object val = entry.getValue().get("defaultParameters");
                        if (val != null) {
                            final String input = val.toString();
                            parameterNamesWithDefaultValue.addAll(Arrays.stream(input.split(","))
                                    .map(String::trim)
                                    .filter(s -> !s.isEmpty())
                                    .collect(Collectors.toList()));
                        }
                    }
                }
            }

            for (VersionedParameter parameter : parameters) {
                if (parameterNamesWithDefaultValue.contains(parameter.getName()) && parameter.getValue() == null) {
                    violations.add("Parameter named `" + parameter.getName() + "` is `null` even though it should have a default value");
                } else if (!parameterNamesWithDefaultValue.contains(parameter.getName()) && parameter.getValue() != null) {
                    violations.add(
                            "Parameter named `" + parameter.getName() + "` is set with value `" + parameter.getValue() + "` and is not configured as a parameter that should have a default value");
                }
            }

            return violations;
        }
    },

    UNUSED_PARAMETER("unusedParameter") {
        @Override
        public List<String> check(final FlowSnapshotContainer container, final String flowName, final RuleConfig config) {
            final List<String> violations = new ArrayList<>();
            final List<VersionedParameter> parameters = container.getFlowSnapshot()
                    .getParameterContexts()
                    .values()
                    .stream()
                    .flatMap(context -> context.getParameters().stream())
                    .toList();

            for (VersionedParameter parameter : parameters) {
                if (!isParameterReferenced(container, parameter.getName())) {
                    violations.add("Parameter named `" + parameter.getName() + "` is not used anywhere in the flow");
                }
            }

            return violations;
        }

        private boolean isParameterReferenced(final FlowSnapshotContainer container, final String parameterName) {
            final ParameterParser parser = new ExpressionLanguageAgnosticParameterParser();
            final VersionedProcessGroup rootProcessGroup = container.getFlowSnapshot().getFlowContents();
            if (rootProcessGroup == null) {
                return false;
            }

            return isParameterReferenced(rootProcessGroup, parameterName, parser);
        }

        private boolean isParameterReferenced(final VersionedProcessGroup group, final String parameterName, final ParameterParser parser) {
            if (group.getProcessors() != null) {
                for (final VersionedProcessor processor : group.getProcessors()) {
                    if (isParameterReferenced(processor.getProperties(), parameterName, parser)) {
                        return true;
                    }
                }
            }

            if (group.getControllerServices() != null) {
                for (final VersionedControllerService service : group.getControllerServices()) {
                    if (isParameterReferenced(service.getProperties(), parameterName, parser)) {
                        return true;
                    }
                }
            }

            if (group.getProcessGroups() != null) {
                for (final VersionedProcessGroup child : group.getProcessGroups()) {
                    if (isParameterReferenced(child, parameterName, parser)) {
                        return true;
                    }
                }
            }

            return false;
        }

        private boolean isParameterReferenced(final Map<String, String> properties, final String parameterName, final ParameterParser parser) {
            if (properties == null) {
                return false;
            }

            for (final String value : properties.values()) {
                if (value == null) {
                    continue;
                }

                final ParameterTokenList tokenList = parser.parseTokens(value);
                for (final ParameterReference reference : tokenList.toReferenceList()) {
                    if (parameterName.equals(reference.getParameterName())) {
                        return true;
                    }
                }
            }

            return false;
        }
    };

    private final String id;

    DefaultCheckstyleRules(final String id) {
        this.id = id;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public boolean ruleApplies(final List<String> includes, final List<String> excludes, final CheckstyleRulesConfig config, final String flowName) {
        if (!includes.contains(id) || excludes.contains(id)) {
            return false;
        }

        if (config == null || config.rules() == null) {
            return true;
        }

        RuleConfig ruleConfig = config.rules().get(id);

        if (ruleConfig == null || ruleConfig.exclude() == null) {
            return true;
        }

        if (flowName == null) {
            return true;
        }

        return ruleConfig.exclude().stream().noneMatch(flowName::matches);
    }

    @Override
    public List<String> check(FlowSnapshotContainer container, String flowName, RuleConfig config) {
        throw new NotImplementedException("This method should be implemented at rule level");
    }

}
