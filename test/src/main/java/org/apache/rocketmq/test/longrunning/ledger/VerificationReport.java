/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.test.longrunning.ledger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class VerificationReport {

    private String runId;
    private String profile;
    private long durationMs;
    private boolean passed;

    private long totalExpectedDelivered;
    private long totalUniqueConsumed;
    private long totalDuplicateConsumed;
    private long totalRolledBack;
    private long totalFilteredOut;
    private long unconsumedCount;

    private long crcCorruptionCount;
    private long fifoOrderViolationCount;
    private long fifoGapViolationCount;
    private long rollbackLeakCount;
    private long filterLeakCount;
    private long prematureTimerDeliveryCount;

    private int e2eCasesTotal;
    private int e2eCasesPassed;
    private int e2eCasesFailed;

    private long p50LatencyMs;
    private long p95LatencyMs;
    private long p99LatencyMs;
    private long maxLatencyMs;

    private int chaosEventsTriggered;
    private int chaosEventsSucceeded;

    private Map<String, Long> sentByWorkload = new LinkedHashMap<>();
    private Map<String, Long> consumedByWorkload = new LinkedHashMap<>();
    private Map<String, String> e2eCaseResults = new LinkedHashMap<>();
    private List<String> violations = new ArrayList<>();
    private List<String> chaosLogs = new ArrayList<>();

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(long durationMs) {
        this.durationMs = durationMs;
    }

    public boolean isPassed() {
        return passed;
    }

    public void setPassed(boolean passed) {
        this.passed = passed;
    }

    public long getTotalExpectedDelivered() {
        return totalExpectedDelivered;
    }

    public void setTotalExpectedDelivered(long totalExpectedDelivered) {
        this.totalExpectedDelivered = totalExpectedDelivered;
    }

    public long getTotalUniqueConsumed() {
        return totalUniqueConsumed;
    }

    public void setTotalUniqueConsumed(long totalUniqueConsumed) {
        this.totalUniqueConsumed = totalUniqueConsumed;
    }

    public long getTotalDuplicateConsumed() {
        return totalDuplicateConsumed;
    }

    public void setTotalDuplicateConsumed(long totalDuplicateConsumed) {
        this.totalDuplicateConsumed = totalDuplicateConsumed;
    }

    public long getTotalRolledBack() {
        return totalRolledBack;
    }

    public void setTotalRolledBack(long totalRolledBack) {
        this.totalRolledBack = totalRolledBack;
    }

    public long getTotalFilteredOut() {
        return totalFilteredOut;
    }

    public void setTotalFilteredOut(long totalFilteredOut) {
        this.totalFilteredOut = totalFilteredOut;
    }

    public long getUnconsumedCount() {
        return unconsumedCount;
    }

    public void setUnconsumedCount(long unconsumedCount) {
        this.unconsumedCount = unconsumedCount;
    }

    public long getCrcCorruptionCount() {
        return crcCorruptionCount;
    }

    public void setCrcCorruptionCount(long crcCorruptionCount) {
        this.crcCorruptionCount = crcCorruptionCount;
    }

    public long getFifoOrderViolationCount() {
        return fifoOrderViolationCount;
    }

    public void setFifoOrderViolationCount(long fifoOrderViolationCount) {
        this.fifoOrderViolationCount = fifoOrderViolationCount;
    }

    public long getFifoGapViolationCount() {
        return fifoGapViolationCount;
    }

    public void setFifoGapViolationCount(long fifoGapViolationCount) {
        this.fifoGapViolationCount = fifoGapViolationCount;
    }

    public long getRollbackLeakCount() {
        return rollbackLeakCount;
    }

    public void setRollbackLeakCount(long rollbackLeakCount) {
        this.rollbackLeakCount = rollbackLeakCount;
    }

    public long getFilterLeakCount() {
        return filterLeakCount;
    }

    public void setFilterLeakCount(long filterLeakCount) {
        this.filterLeakCount = filterLeakCount;
    }

    public int getE2eCasesTotal() {
        return e2eCasesTotal;
    }

    public void setE2eCasesTotal(int e2eCasesTotal) {
        this.e2eCasesTotal = e2eCasesTotal;
    }

    public int getE2eCasesPassed() {
        return e2eCasesPassed;
    }

    public void setE2eCasesPassed(int e2eCasesPassed) {
        this.e2eCasesPassed = e2eCasesPassed;
    }

    public int getE2eCasesFailed() {
        return e2eCasesFailed;
    }

    public void setE2eCasesFailed(int e2eCasesFailed) {
        this.e2eCasesFailed = e2eCasesFailed;
    }

    public Map<String, String> getE2eCaseResults() {
        return e2eCaseResults;
    }

    public void setE2eCaseResults(Map<String, String> e2eCaseResults) {
        this.e2eCaseResults = e2eCaseResults;
    }

    public long getPrematureTimerDeliveryCount() {
        return prematureTimerDeliveryCount;
    }

    public void setPrematureTimerDeliveryCount(long prematureTimerDeliveryCount) {
        this.prematureTimerDeliveryCount = prematureTimerDeliveryCount;
    }

    public long getP50LatencyMs() {
        return p50LatencyMs;
    }

    public void setP50LatencyMs(long p50LatencyMs) {
        this.p50LatencyMs = p50LatencyMs;
    }

    public long getP95LatencyMs() {
        return p95LatencyMs;
    }

    public void setP95LatencyMs(long p95LatencyMs) {
        this.p95LatencyMs = p95LatencyMs;
    }

    public long getP99LatencyMs() {
        return p99LatencyMs;
    }

    public void setP99LatencyMs(long p99LatencyMs) {
        this.p99LatencyMs = p99LatencyMs;
    }

    public long getMaxLatencyMs() {
        return maxLatencyMs;
    }

    public void setMaxLatencyMs(long maxLatencyMs) {
        this.maxLatencyMs = maxLatencyMs;
    }

    public int getChaosEventsTriggered() {
        return chaosEventsTriggered;
    }

    public void setChaosEventsTriggered(int chaosEventsTriggered) {
        this.chaosEventsTriggered = chaosEventsTriggered;
    }

    public int getChaosEventsSucceeded() {
        return chaosEventsSucceeded;
    }

    public void setChaosEventsSucceeded(int chaosEventsSucceeded) {
        this.chaosEventsSucceeded = chaosEventsSucceeded;
    }

    public Map<String, Long> getSentByWorkload() {
        return sentByWorkload;
    }

    public void setSentByWorkload(Map<String, Long> sentByWorkload) {
        this.sentByWorkload = sentByWorkload;
    }

    public Map<String, Long> getConsumedByWorkload() {
        return consumedByWorkload;
    }

    public void setConsumedByWorkload(Map<String, Long> consumedByWorkload) {
        this.consumedByWorkload = consumedByWorkload;
    }

    public List<String> getViolations() {
        return violations;
    }

    public void setViolations(List<String> violations) {
        this.violations = violations;
    }

    public List<String> getChaosLogs() {
        return chaosLogs;
    }

    public void setChaosLogs(List<String> chaosLogs) {
        this.chaosLogs = chaosLogs;
    }
}
