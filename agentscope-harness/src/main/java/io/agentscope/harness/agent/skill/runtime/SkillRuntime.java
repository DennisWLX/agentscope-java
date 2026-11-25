/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.harness.agent.skill.runtime;

import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolResultState;
import io.agentscope.core.skill.SkillFilter;
import io.agentscope.core.tool.AgentTool;
import io.agentscope.core.tool.Toolkit;
import io.agentscope.harness.agent.context.ContextItem;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

/**
 * Aggregates the singleton {@link SkillLoadTool} and the {@link SkillPromptBuilder}. {@link
 * io.agentscope.harness.agent.middleware.HarnessSkillMiddleware} owns one instance and installs
 * each filtered {@link SkillCatalog} into that call's {@link RuntimeContext}.
 *
 * <p>The {@code load_skill_through_path} tool is prepared during agent construction and verified
 * idempotently on later installs. Normal tool calls resolve the catalog from their {@code
 * RuntimeContext}, so concurrent sessions cannot overwrite or observe one another's filtered skill
 * view. A separate compatibility catalog is retained only for deprecated, context-free callers.
 */
public final class SkillRuntime {

    private static final Logger log = LoggerFactory.getLogger(SkillRuntime.class);

    private final RuntimeContext legacyContext = RuntimeContext.empty();
    private final AtomicReference<SkillCatalog> legacyCatalogRef =
            new AtomicReference<>(SkillCatalog.empty());
    private final SkillLoadTool loadTool;
    private final SkillPromptBuilder promptBuilder;

    /** Binds recovery to one subscribed Harness execution, including error and cancellation. */
    public static <T> Flux<T> withRecoveryScope(
            Agent agent, RuntimeContext context, Supplier<Flux<T>> execution) {
        if (agent == null || context == null) return Flux.defer(execution);
        return Flux.using(
                () -> {
                    LoadedMaterials materials = new LoadedMaterials(agent);
                    context.put(LoadedMaterials.class, materials);
                    return materials;
                },
                materials -> execution.get(),
                LoadedMaterials::close);
    }

    static void recordLoad(
            Agent agent,
            RuntimeContext context,
            String callId,
            String skillId,
            String path,
            ToolResultBlock result) {
        LoadedMaterials materials = recoveryScope(agent, context);
        if (materials == null || callId == null) return;
        String text = receiptText(result);
        if (text == null) return;
        materials.stage(
                callId,
                ContextItem.runtimeState(
                        "loaded_skill",
                        "harness:loaded_skill:" + skillId + "/" + path,
                        true,
                        text));
    }

    /** Confirms that a native load actually reached the request before Harness reduced it. */
    public static List<ContextItem> loadedMaterials(
            Agent agent, RuntimeContext context, List<Msg> request, List<Msg> history) {
        LoadedMaterials materials = recoveryScope(agent, context);
        return materials == null ? List.of() : materials.confirm(request, history);
    }

    /** Returns loaded documents without a complete receipt in the final request. */
    public static List<ContextItem> missingMaterials(List<ContextItem> loaded, List<Msg> messages) {
        if (loaded.isEmpty()) return List.of();
        Set<String> visible = new HashSet<>(receipts(messages).values());
        return loaded.stream().filter(item -> !visible.contains(item.content())).toList();
    }

    private static LoadedMaterials recoveryScope(Agent agent, RuntimeContext context) {
        LoadedMaterials scope = context == null ? null : context.get(LoadedMaterials.class);
        return agent != null && scope != null && scope.owner == agent ? scope : null;
    }

    private static Map<String, String> receipts(List<Msg> messages) {
        Map<String, String> receipts = new LinkedHashMap<>();
        for (Msg message : messages) {
            if (message.getRole() != MsgRole.TOOL) continue;
            for (ToolResultBlock result : message.getContentBlocks(ToolResultBlock.class)) {
                if (!SkillLoadTool.TOOL_NAME.equals(result.getName())) continue;
                String text = receiptText(result);
                if (text != null) receipts.put(result.getId(), text);
            }
        }
        return receipts;
    }

    private static String receiptText(ToolResultBlock result) {
        return (result.getState() == ToolResultState.SUCCESS
                                || result.getState() == ToolResultState.RUNNING)
                        && result.getOutput().size() == 1
                        && result.getOutput().get(0) instanceof TextBlock text
                ? text.getText()
                : null;
    }

    private static final class LoadedMaterials {
        private record Revision(ContextItem item, Map<String, Boolean> eligibleByCallId) {}

        private volatile Agent owner;
        private final Map<String, Revision> latestBySource = new LinkedHashMap<>();

        private LoadedMaterials(Agent owner) {
            this.owner = owner;
        }

        private synchronized void stage(String callId, ContextItem item) {
            if (owner == null) return;
            Revision latest = latestBySource.get(item.sourceId());
            if (latest == null || !latest.item().revision().equals(item.revision())) {
                latest = new Revision(item, new HashMap<>());
                latestBySource.put(item.sourceId(), latest);
            }
            latest.eligibleByCallId().put(callId, false);
        }

        private synchronized List<ContextItem> confirm(List<Msg> request, List<Msg> history) {
            if (latestBySource.isEmpty()) return List.of();
            Map<String, String> visible = receipts(request);
            Map<String, String> inHistory = receipts(history);
            List<ContextItem> eligible = new ArrayList<>();
            for (Revision latest : latestBySource.values()) {
                Map<String, Boolean> calls = latest.eligibleByCallId();
                for (Map.Entry<String, Boolean> call : calls.entrySet()) {
                    String id = call.getKey();
                    String text = visible.get(id);
                    if (latest.item().content().equals(text)) {
                        call.setValue(true);
                    } else if (!Objects.equals(inHistory.get(id), text)) {
                        // Request-only filtering suspends this receipt until its full text returns.
                        call.setValue(false);
                    }
                }
                if (calls.containsValue(true)) eligible.add(latest.item());
            }
            return List.copyOf(eligible);
        }

        private synchronized void close() {
            // A caller or tool may retain a copy of the context beyond this execution.
            owner = null;
            latestBySource.clear();
        }
    }

    public SkillRuntime() {
        this(new SkillPromptBuilder());
    }

    public SkillRuntime(SkillPromptBuilder promptBuilder) {
        this.promptBuilder = promptBuilder != null ? promptBuilder : new SkillPromptBuilder();
        this.loadTool = new SkillLoadTool(legacyCatalogRef, false);
    }

    /**
     * Returns the catalog installed through the deprecated context-free API.
     *
     * @deprecated Use {@link #currentCatalog(RuntimeContext)} so catalog lookup is request-scoped.
     */
    @Deprecated(since = "2.2.0")
    public SkillCatalog currentCatalog() {
        return currentCatalog(legacyContext);
    }

    /** Returns the catalog bound to {@code context}, or an empty catalog when none is bound. */
    public SkillCatalog currentCatalog(RuntimeContext context) {
        return catalogFor(context);
    }

    static SkillCatalog catalogFor(RuntimeContext context) {
        if (context == null) {
            return SkillCatalog.empty();
        }
        SkillCatalog catalog = context.get(SkillCatalog.class);
        return catalog != null ? catalog : SkillCatalog.empty();
    }

    /**
     * Underlying stateless tool instance. Use {@link #install(SkillCatalog, RuntimeContext,
     * Toolkit)} for normal flow.
     */
    public AgentTool loadTool() {
        return loadTool;
    }

    /**
     * Ensures the skill loading tool is present as an ungrouped tool.
     *
     * <p>Harness calls this while the agent toolkit is still being assembled. The toolkit copy
     * made by {@code ReActAgent.Builder} therefore contains the tool before its first model call.
     * Keeping the loader ungrouped is intentional: ungrouped tools remain visible regardless of a
     * session's persisted active-group list, including sessions created before the loader existed.
     */
    public void prepareToolkit(Toolkit toolkit) {
        if (toolkit == null) {
            return;
        }
        try {
            AgentTool existing = toolkit.getTool(SkillLoadTool.TOOL_NAME);
            if (existing == loadTool) {
                return;
            }
            if (existing != null) {
                toolkit.removeTool(SkillLoadTool.TOOL_NAME);
            }
            toolkit.registration().agentTool(loadTool).apply();
        } catch (Exception e) {
            log.warn("Failed to register {}: {}", SkillLoadTool.TOOL_NAME, e.getMessage());
        }
    }

    /**
     * Bind a catalog to the current call and ensure the load tool is registered on the toolkit
     * (idempotent).
     *
     * <p>The catalog is deliberately stored on {@code context}, not on this shared runtime. This is
     * the authorization boundary used by {@link SkillLoadTool}; a skill omitted from the call's
     * filtered catalog cannot be loaded even while another session exposes it.
     *
     * @param catalog the call snapshot; pass {@link SkillCatalog#empty()} to clear visibility
     * @param context the current call context; may be {@code null}, in which case no catalog is
     *     exposed
     * @param toolkit the toolkit to install onto; may be {@code null} (then only catalog is updated)
     */
    public void install(SkillCatalog catalog, RuntimeContext context, Toolkit toolkit) {
        if (context != null) {
            context.put(SkillCatalog.class, catalog != null ? catalog : SkillCatalog.empty());
        }
        prepareToolkit(toolkit);
    }

    /**
     * Installs a catalog for legacy context-free callers.
     *
     * <p>This overload is retained for source and binary compatibility. Its catalog is used only
     * when {@link SkillLoadTool} is invoked without a {@link RuntimeContext}; context-bearing calls
     * never fall back to it and therefore remain isolated.
     *
     * @deprecated Use {@link #install(SkillCatalog, RuntimeContext, Toolkit)}.
     */
    @Deprecated(since = "2.2.0")
    public void install(SkillCatalog catalog, Toolkit toolkit) {
        SkillCatalog normalized = catalog != null ? catalog : SkillCatalog.empty();
        legacyCatalogRef.set(normalized);
        install(normalized, legacyContext, toolkit);
    }

    /** Renders the {@code <available_skills>} block + (when applicable) the code-execution prompt. */
    public String renderPrompt(SkillCatalog catalog, SkillFilter filter) {
        return promptBuilder.render(
                catalog != null ? catalog : SkillCatalog.empty(),
                filter != null ? filter : SkillFilter.all());
    }
}
