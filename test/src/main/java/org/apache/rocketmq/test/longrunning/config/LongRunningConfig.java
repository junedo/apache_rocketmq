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
package org.apache.rocketmq.test.longrunning.config;

import java.util.UUID;

public class LongRunningConfig {

    private String runId = "lr-" + UUID.randomUUID().toString().substring(0, 8);
    private String namesrvAddr = "127.0.0.1:9876";
    private String clusterName = "DefaultCluster";
    private StorageProfile profile = StorageProfile.DEFAULT;
    private String workDir = System.getProperty("java.io.tmpdir") + "/rmq-e2e-longrunning";
    private String clusterScriptPath = "";

    private long durationSeconds = 30L;
    private long drainTimeoutSeconds = 45L;
    private long chaosIntervalSeconds = 10L;
    private String chaosMode = "graceful";

    private int queueNums = 4;
    private int sendIntervalMs = 25;
    private int payloadSizeBytes = 256;
    private int fifoKeyCount = 8;
    private long delayMessageMs = 2000L;
    private long timerToleranceMs = 1500L;

    private boolean enableNormalWorkload = true;
    private boolean enableFifoWorkload = true;
    private boolean enableDelayWorkload = true;
    private boolean enableTransactionWorkload = true;
    private boolean enablePopWorkload = true;

    public static LongRunningConfig fromArgs(String[] args) {
        LongRunningConfig cfg = new LongRunningConfig();
        if (args == null) {
            return cfg;
        }
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String next = (i + 1 < args.length) ? args[i + 1] : "";
            switch (arg) {
                case "--runId":
                    cfg.setRunId(next);
                    i++;
                    break;
                case "--namesrvAddr":
                    cfg.setNamesrvAddr(next);
                    i++;
                    break;
                case "--clusterName":
                    cfg.setClusterName(next);
                    i++;
                    break;
                case "--profile":
                    cfg.setProfile(StorageProfile.fromString(next));
                    i++;
                    break;
                case "--workDir":
                    cfg.setWorkDir(next);
                    i++;
                    break;
                case "--clusterScript":
                    cfg.setClusterScriptPath(next);
                    i++;
                    break;
                case "--duration":
                case "--durationSeconds":
                    cfg.setDurationSeconds(Long.parseLong(next));
                    i++;
                    break;
                case "--drainTimeoutSeconds":
                    cfg.setDrainTimeoutSeconds(Long.parseLong(next));
                    i++;
                    break;
                case "--chaosInterval":
                case "--chaosIntervalSeconds":
                    cfg.setChaosIntervalSeconds(Long.parseLong(next));
                    i++;
                    break;
                case "--chaosMode":
                    cfg.setChaosMode(next);
                    i++;
                    break;
                case "--queueNums":
                    cfg.setQueueNums(Integer.parseInt(next));
                    i++;
                    break;
                case "--sendIntervalMs":
                    cfg.setSendIntervalMs(Integer.parseInt(next));
                    i++;
                    break;
                case "--payloadSize":
                    cfg.setPayloadSizeBytes(Integer.parseInt(next));
                    i++;
                    break;
                default:
                    break;
            }
        }
        return cfg;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public StorageProfile getProfile() {
        return profile;
    }

    public void setProfile(StorageProfile profile) {
        this.profile = profile;
    }

    public String getWorkDir() {
        return workDir;
    }

    public void setWorkDir(String workDir) {
        this.workDir = workDir;
    }

    public String getClusterScriptPath() {
        return clusterScriptPath;
    }

    public void setClusterScriptPath(String clusterScriptPath) {
        this.clusterScriptPath = clusterScriptPath;
    }

    public long getDurationSeconds() {
        return durationSeconds;
    }

    public void setDurationSeconds(long durationSeconds) {
        this.durationSeconds = durationSeconds;
    }

    public long getDrainTimeoutSeconds() {
        return drainTimeoutSeconds;
    }

    public void setDrainTimeoutSeconds(long drainTimeoutSeconds) {
        this.drainTimeoutSeconds = drainTimeoutSeconds;
    }

    public long getChaosIntervalSeconds() {
        return chaosIntervalSeconds;
    }

    public void setChaosIntervalSeconds(long chaosIntervalSeconds) {
        this.chaosIntervalSeconds = chaosIntervalSeconds;
    }

    public String getChaosMode() {
        return chaosMode;
    }

    public void setChaosMode(String chaosMode) {
        this.chaosMode = chaosMode;
    }

    public int getQueueNums() {
        return queueNums;
    }

    public void setQueueNums(int queueNums) {
        this.queueNums = queueNums;
    }

    public int getSendIntervalMs() {
        return sendIntervalMs;
    }

    public void setSendIntervalMs(int sendIntervalMs) {
        this.sendIntervalMs = sendIntervalMs;
    }

    public int getPayloadSizeBytes() {
        return payloadSizeBytes;
    }

    public void setPayloadSizeBytes(int payloadSizeBytes) {
        this.payloadSizeBytes = payloadSizeBytes;
    }

    public int getFifoKeyCount() {
        return fifoKeyCount;
    }

    public void setFifoKeyCount(int fifoKeyCount) {
        this.fifoKeyCount = fifoKeyCount;
    }

    public long getDelayMessageMs() {
        return delayMessageMs;
    }

    public void setDelayMessageMs(long delayMessageMs) {
        this.delayMessageMs = delayMessageMs;
    }

    public long getTimerToleranceMs() {
        return timerToleranceMs;
    }

    public void setTimerToleranceMs(long timerToleranceMs) {
        this.timerToleranceMs = timerToleranceMs;
    }

    public boolean isEnableNormalWorkload() {
        return enableNormalWorkload;
    }

    public void setEnableNormalWorkload(boolean enableNormalWorkload) {
        this.enableNormalWorkload = enableNormalWorkload;
    }

    public boolean isEnableFifoWorkload() {
        return enableFifoWorkload;
    }

    public void setEnableFifoWorkload(boolean enableFifoWorkload) {
        this.enableFifoWorkload = enableFifoWorkload;
    }

    public boolean isEnableDelayWorkload() {
        return enableDelayWorkload;
    }

    public void setEnableDelayWorkload(boolean enableDelayWorkload) {
        this.enableDelayWorkload = enableDelayWorkload;
    }

    public boolean isEnableTransactionWorkload() {
        return enableTransactionWorkload;
    }

    public void setEnableTransactionWorkload(boolean enableTransactionWorkload) {
        this.enableTransactionWorkload = enableTransactionWorkload;
    }

    public boolean isEnablePopWorkload() {
        return enablePopWorkload;
    }

    public void setEnablePopWorkload(boolean enablePopWorkload) {
        this.enablePopWorkload = enablePopWorkload;
    }
}
