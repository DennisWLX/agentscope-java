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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.ReActAgent;
import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.core.skill.AgentSkill;
import io.agentscope.core.skill.repository.AgentSkillRepository;
import io.agentscope.core.tool.Toolkit;
import io.agentscope.core.util.JsonUtils;
import io.agentscope.harness.agent.context.ContextBudgetExceededException;
import io.agentscope.harness.agent.context.ContextItem;
import io.agentscope.harness.agent.context.ContextManifest;
import io.agentscope.harness.agent.context.ContextPolicy;
import io.agentscope.harness.agent.context.ContextTokenEstimator;
import io.agentscope.harness.agent.context.HarnessContextBuilder;
import io.agentscope.harness.agent.memory.compaction.CompactionConfig;
import io.agentscope.harness.agent.middleware.CompactionMiddleware;
import io.agentscope.harness.agent.middleware.HarnessSkillMiddleware;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

class SkillMaterialRecoveryTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final String SKILL_TEXT = "Finish the checklist after review. ".repeat(16);
    private static final String REFERENCE_TEXT =
            "Do not overwrite the previous export. ".repeat(16);
    private static final String REFERENCE = "references/checklist.md";
    private static final String NOTES = "notes.txt";
    private static final String TOOL_NAME = SkillLoadTool.TOOL_NAME;

    private final AgentSkill skill =
            AgentSkill.builder()
                    .name("checklist")
                    .source("test")
                    .description("Example checklist")
                    .skillContent(SKILL_TEXT)
                    .addResource(REFERENCE, REFERENCE_TEXT)
                    .addResource(NOTES, "New tool output. ".repeat(500))
                    .build();
    private final RuntimeContext context = RuntimeContext.builder().sessionId("checklist").build();
    private final List<ContextManifest> manifests = new ArrayList<>();
    private final List<List<Msg>> requests = new ArrayList<>();

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void restoresMissingDocumentsAfterPruningOrSummarizingOnlyWithinTheCall(boolean summarize) {
        try (ReActAgent agent = agent(summarize, false)) {
            assertEquals(
                    "done",
                    agent.call("work on the checklist", context).block(TIMEOUT).getTextContent());
            assertEquals(4, requests.size());
            List<Msg> before = requests.get(1);
            assertFalse(before.stream().anyMatch(SkillMaterialRecoveryTest::isRecovery));
            assertTrue(hasToolText(before, SKILL_TEXT.strip()));
            assertTrue(hasToolText(before, REFERENCE_TEXT.strip()));

            List<Msg> after = requests.get(2);
            Msg recovery = after.get(after.size() - 1);
            assertTrue(isRecovery(recovery));
            assertEquals(1, after.stream().filter(SkillMaterialRecoveryTest::isRecovery).count());
            assertTrue(recovery.getTextContent().contains(SKILL_TEXT.strip()));
            assertTrue(recovery.getTextContent().contains(REFERENCE_TEXT.strip()));
            assertFalse(hasToolText(after, SKILL_TEXT.strip()));
            assertFalse(hasToolText(after, REFERENCE_TEXT.strip()));
            assertTrue(recovery.getTextContent().contains(REFERENCE));
            assertFalse(recovery.getTextContent().contains(NOTES));
            List<Msg> next = requests.get(3);
            assertEquals(
                    List.of(recovery.getTextContent()),
                    next.stream()
                            .filter(SkillMaterialRecoveryTest::isRecovery)
                            .map(Msg::getTextContent)
                            .toList());
            assertTrue(isRecovery(next.get(next.size() - 1)));
            for (int i = 0; i < 2; i++) {
                assertEquals(before.get(i).getRole(), after.get(i).getRole());
                assertEquals(before.get(i).getContent(), after.get(i).getContent());
            }
            assertEquals(
                    "newer-result", toolResults(after).get(toolResults(after).size() - 1).getId());
            assertFalse(
                    context.getAgentState().contextMutable().stream()
                            .anyMatch(SkillMaterialRecoveryTest::isRecovery));
            assertTrue(
                    manifests.stream()
                            .anyMatch(
                                    manifest -> manifest.transforms().contains("skill_recovery")));

            agent.streamEvents(
                            Msg.builder().role(MsgRole.USER).textContent("new call").build(),
                            context)
                    .then()
                    .block(TIMEOUT);
            assertEquals(5, requests.size());
            assertFalse(requests.get(4).stream().anyMatch(SkillMaterialRecoveryTest::isRecovery));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void filteredReceiptCanRecoverOnlyAfterTheOriginalIsVisibleAgain(boolean previouslyConfirmed) {
        Agent agent = mock(Agent.class);
        String callId = "skill";
        ToolResultBlock result =
                ToolResultBlock.success(SKILL_TEXT).withIdAndName(callId, TOOL_NAME);
        List<Msg> original = receipt(result);
        List<Msg> filtered =
                receipt(ToolResultBlock.success("filtered").withIdAndName(callId, TOOL_NAME));
        List<Msg> differentCall = receipt(result.withIdAndName("other", TOOL_NAME));
        SkillRuntime.withRecoveryScope(
                        agent,
                        context,
                        () -> {
                            SkillRuntime.recordLoad(
                                    agent, context, callId, skill.getSkillId(), "SKILL.md", result);
                            if (previouslyConfirmed) {
                                assertEquals(
                                        List.of(SKILL_TEXT),
                                        recoverableAfterReduction(agent, original, original));
                            }
                            assertTrue(
                                    recoverableAfterReduction(agent, differentCall, original)
                                            .isEmpty());
                            assertTrue(
                                    recoverableAfterReduction(agent, filtered, original).isEmpty());
                            assertTrue(
                                    recoverableAfterReduction(agent, filtered, filtered).isEmpty());
                            assertTrue(
                                    recoverableAfterReduction(agent, List.of(), List.of())
                                            .isEmpty());
                            assertEquals(
                                    List.of(SKILL_TEXT),
                                    recoverableAfterReduction(agent, original, original));
                            assertEquals(
                                    List.of(SKILL_TEXT),
                                    recoverableAfterReduction(agent, List.of(), List.of()));
                            return Flux.empty();
                        })
                .blockLast(TIMEOUT);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void recoveryUsesOnlyReceiptsOfTheLatestVersion(boolean versionChanged) {
        Agent agent = mock(Agent.class);
        String firstText = "original checklist";
        String latestText = versionChanged ? "updated checklist" : firstText;
        ToolResultBlock first =
                ToolResultBlock.success(firstText).withIdAndName("first", TOOL_NAME);
        ToolResultBlock latest =
                ToolResultBlock.success(latestText).withIdAndName("latest", TOOL_NAME);
        Msg original = receipt(first).get(0);
        Msg updated = receipt(latest).get(0);
        Msg filtered =
                receipt(
                                ToolResultBlock.success("filtered")
                                        .withIdAndName(latest.getId(), TOOL_NAME))
                        .get(0);
        List<Msg> history = List.of(original, updated);
        List<Msg> requestWithFilter = List.of(original, filtered);
        List<String> expectedWhileFiltered = versionChanged ? List.of() : List.of(firstText);

        SkillRuntime.withRecoveryScope(
                        agent,
                        context,
                        () -> {
                            SkillRuntime.recordLoad(
                                    agent,
                                    context,
                                    first.getId(),
                                    skill.getSkillId(),
                                    REFERENCE,
                                    first);
                            assertEquals(
                                    List.of(firstText),
                                    recoverableAfterReduction(
                                            agent, List.of(original), List.of(original)));
                            SkillRuntime.recordLoad(
                                    agent,
                                    context,
                                    latest.getId(),
                                    skill.getSkillId(),
                                    REFERENCE,
                                    latest);
                            assertEquals(
                                    expectedWhileFiltered,
                                    recoverableAfterReduction(agent, requestWithFilter, history));
                            assertEquals(
                                    List.of(latestText),
                                    recoverableAfterReduction(agent, history, history));
                            assertEquals(
                                    expectedWhileFiltered,
                                    recoverableAfterReduction(agent, requestWithFilter, history));
                            assertEquals(
                                    expectedWhileFiltered,
                                    recoverableAfterReduction(agent, List.of(), List.of()));
                            assertEquals(
                                    List.of(latestText),
                                    recoverableAfterReduction(agent, history, history));
                            return Flux.empty();
                        })
                .blockLast(TIMEOUT);
    }

    private List<String> recoverableAfterReduction(
            Agent agent, List<Msg> request, List<Msg> history) {
        List<ContextItem> eligible = SkillRuntime.loadedMaterials(agent, context, request, history);
        return SkillRuntime.missingMaterials(eligible, List.of()).stream()
                .map(ContextItem::content)
                .toList();
    }

    private static List<Msg> receipt(ToolResultBlock result) {
        return List.of(Msg.builder().role(MsgRole.TOOL).content(result).build());
    }

    @Test
    void cancellationRetiresRecoveryEvenForRetainedToolContexts() {
        Agent agent = mock(Agent.class);
        ToolResultBlock result =
                ToolResultBlock.success(SKILL_TEXT).withIdAndName("skill", TOOL_NAME);
        List<Msg> history = receipt(result);
        Disposable execution =
                SkillRuntime.withRecoveryScope(agent, context, Flux::never).subscribe();
        RuntimeContext toolContext = RuntimeContext.builder(context).build();
        try {
            SkillRuntime.recordLoad(
                    agent, toolContext, result.getId(), skill.getSkillId(), REFERENCE, result);
            assertEquals(1, SkillRuntime.loadedMaterials(agent, context, history, history).size());
        } finally {
            execution.dispose();
        }
        // A late tool completion must not reopen recovery through its retained context copy.
        SkillRuntime.recordLoad(
                agent, toolContext, result.getId(), skill.getSkillId(), REFERENCE, result);
        assertTrue(SkillRuntime.loadedMaterials(agent, toolContext, history, history).isEmpty());
    }

    @Test
    void rejectsOverBudgetRecoveryBeforeModelInvocation() {
        try (ReActAgent agent = agent(false, true)) {
            assertThrows(
                    ContextBudgetExceededException.class,
                    () -> agent.call("work on the checklist", context).block(TIMEOUT));
            assertEquals(2, requests.size());
            ContextManifest rejected = manifests.get(manifests.size() - 1);
            assertEquals("budget_exceeded", rejected.validation());
            assertTrue(rejected.transforms().contains("skill_recovery"));
        }
    }

    private ReActAgent agent(boolean summarize, boolean rejectRecovery) {
        Toolkit toolkit = new Toolkit();
        AgentSkillRepository repository = mock(AgentSkillRepository.class);
        when(repository.getAllSkills()).thenReturn(List.of(skill));
        HarnessSkillMiddleware middleware =
                new HarnessSkillMiddleware(List.of(repository), toolkit);
        Model model =
                new Model() {
                    @Override
                    public String getModelName() {
                        return "scripted";
                    }

                    @Override
                    public Flux<ChatResponse> stream(
                            List<Msg> messages, List<ToolSchema> tools, GenerateOptions options) {
                        requests.add(List.copyOf(messages));
                        return switch (requests.size()) {
                            case 1 ->
                                    response(
                                            load("skill", "SKILL.md"),
                                            load("reference", REFERENCE));
                            case 2 -> response(load("newer-result", NOTES));
                            case 3 -> response(load("latest-result", NOTES));
                            default -> response(TextBlock.builder().text("done").build());
                        };
                    }
                };
        Model summary = mock(Model.class);
        when(summary.getModelName()).thenReturn("summary");
        when(summary.stream(anyList(), any(), any()))
                .thenAnswer(
                        ignored ->
                                response(
                                        TextBlock.builder()
                                                .text("Previously read the checklist.")
                                                .build()));
        CompactionConfig config =
                CompactionConfig.builder()
                        .triggerMessages(summarize ? 5 : 0)
                        .keepMessages(2)
                        .keepTokens(0)
                        .flushBeforeCompact(false)
                        .truncateArgs(null)
                        .prune(
                                summarize
                                        ? null
                                        : CompactionConfig.PruneConfig.builder()
                                                .protectTokens(1_000)
                                                .minimumTokens(1)
                                                .maxOutputChars(100)
                                                .build())
                        .build();
        ContextTokenEstimator estimator = ContextTokenEstimator.approximate();
        ContextPolicy policy =
                new ContextPolicy(
                        16_000,
                        1,
                        1,
                        request ->
                                rejectRecovery
                                                && request.messages().stream()
                                                        .anyMatch(
                                                                SkillMaterialRecoveryTest
                                                                        ::isRecovery)
                                        ? new ContextTokenEstimator.Estimate(16_001, "test", false)
                                        : estimator.estimate(request),
                        manifests::add);
        return ReActAgent.builder()
                .name("skill-recovery-test")
                .sysPrompt("Use the checklist.")
                .model(model)
                .toolkit(toolkit)
                .middleware(middleware)
                .modelRequestPreparer(
                        new HarnessContextBuilder(
                                policy, new CompactionMiddleware(null, summary, config), null))
                .build();
    }

    private ToolUseBlock load(String id, String path) {
        Map<String, Object> input = Map.of("skillId", skill.getSkillId(), "path", path);
        return ToolUseBlock.builder()
                .id(id)
                .name(TOOL_NAME)
                .input(input)
                .content(JsonUtils.getJsonCodec().toJson(input))
                .build();
    }

    private static Flux<ChatResponse> response(ContentBlock... blocks) {
        return Flux.just(ChatResponse.builder().content(List.of(blocks)).build());
    }

    private static boolean hasToolText(List<Msg> messages, String text) {
        return toolResults(messages).stream()
                .flatMap(result -> result.getOutput().stream())
                .anyMatch(
                        block ->
                                block instanceof TextBlock output
                                        && output.getText().contains(text));
    }

    private static List<ToolResultBlock> toolResults(List<Msg> messages) {
        return messages.stream()
                .flatMap(msg -> msg.getContentBlocks(ToolResultBlock.class).stream())
                .toList();
    }

    private static boolean isRecovery(Msg msg) {
        return "skill_recovery".equals(msg.getMetadata().get(Msg.METADATA_REMINDER_KIND));
    }
}
