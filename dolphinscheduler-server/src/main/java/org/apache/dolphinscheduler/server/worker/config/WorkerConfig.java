
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.server.worker.config;

import org.apache.dolphinscheduler.common.Constants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource(value = "worker.properties")
public class WorkerConfig {

    @Value("${worker.exec.threads:100}")
    private int workerExecThreads;

    @Value("${worker.heartbeat.interval:10}")
    private int workerHeartbeatInterval;

    @Value("${worker.fetch.task.num:3}")
    private int workerFetchTaskNum;

    @Value("${worker.max.cpuload.avg:-1}")
    private int workerMaxCpuloadAvg;

    @Value("${worker.reserved.memory:0.3}")
    private double workerReservedMemory;

    @Value("${worker.group: default}")
    private String workerGroup;

    @Value("${worker.listen.port: 1234}")
    private int listenPort;

    @Value("${sqoop.exec.threads:5}")
    private int sqoopExecThreads;

    @Value("${datax.exec.threads:5}")
    private int dataXExceThreads;

    @Value("${http.exec.threads:5}")
    private int httpExecThreads;

    @Value("${python.exec.threads:5}")
    private int pythonExecThreads;

    @Value("${mr.exec.threads:5}")
    private int mrExecThreads;

    @Value("${flink.exec.threads:5}")
    private int flinkExecThreads;

    @Value("${spark.exec.threads:5}")
    private int sparkExecThreads;

    @Value("${sql.exec.threads:5}")
    private int sqlExecThreads;

    @Value("${proceduer.exec.threads:5}")
    private int prodecuerExecThreads;

    @Value("${shell.exec.threads:5}")
    private int shellExecThreads;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public String getWorkerGroup() {
        return workerGroup;
    }

    public void setWorkerGroup(String workerGroup) {
        this.workerGroup = workerGroup;
    }

    public int getWorkerExecThreads() {
        return workerExecThreads;
    }

    public void setWorkerExecThreads(int workerExecThreads) {
        this.workerExecThreads = workerExecThreads;
    }

    public int getWorkerHeartbeatInterval() {
        return workerHeartbeatInterval;
    }

    public void setWorkerHeartbeatInterval(int workerHeartbeatInterval) {
        this.workerHeartbeatInterval = workerHeartbeatInterval;
    }

    public int getWorkerFetchTaskNum() {
        return workerFetchTaskNum;
    }

    public void setWorkerFetchTaskNum(int workerFetchTaskNum) {
        this.workerFetchTaskNum = workerFetchTaskNum;
    }

    public double getWorkerReservedMemory() {
        return workerReservedMemory;
    }

    public void setWorkerReservedMemory(double workerReservedMemory) {
        this.workerReservedMemory = workerReservedMemory;
    }

    public int getWorkerMaxCpuloadAvg() {
        if (workerMaxCpuloadAvg == -1){
            return Constants.DEFAULT_WORKER_CPU_LOAD;
        }
        return workerMaxCpuloadAvg;
    }

    public void setWorkerMaxCpuloadAvg(int workerMaxCpuloadAvg) {
        this.workerMaxCpuloadAvg = workerMaxCpuloadAvg;
    }

    public int getSqoopExecThreads() {
        return sqoopExecThreads;
    }

    public void setSqoopExecThreads(int sqoopExecThreads) {
        this.sqoopExecThreads = sqoopExecThreads;
    }

    public int getDataXExceThreads() {
        return dataXExceThreads;
    }

    public void setDataXExceThreads(int dataXExceThreads) {
        this.dataXExceThreads = dataXExceThreads;
    }

    public int getHttpExecThreads() {
        return httpExecThreads;
    }

    public void setHttpExecThreads(int httpExecThreads) {
        this.httpExecThreads = httpExecThreads;
    }

    public int getPythonExecThreads() {
        return pythonExecThreads;
    }

    public void setPythonExecThreads(int pythonExecThreads) {
        this.pythonExecThreads = pythonExecThreads;
    }

    public int getMrExecThreads() {
        return mrExecThreads;
    }

    public void setMrExecThreads(int mrExecThreads) {
        this.mrExecThreads = mrExecThreads;
    }

    public int getFlinkExecThreads() {
        return flinkExecThreads;
    }

    public void setFlinkExecThreads(int flinkExecThreads) {
        this.flinkExecThreads = flinkExecThreads;
    }

    public int getSparkExecThreads() {
        return sparkExecThreads;
    }

    public void setSparkExecThreads(int sparkExecThreads) {
        this.sparkExecThreads = sparkExecThreads;
    }

    public int getSqlExecThreads() {
        return sqlExecThreads;
    }

    public void setSqlExecThreads(int sqlExecThreads) {
        this.sqlExecThreads = sqlExecThreads;
    }

    public int getProdecuerExecThreads() {
        return prodecuerExecThreads;
    }

    public void setProdecuerExecThreads(int prodecuerExecThreads) {
        this.prodecuerExecThreads = prodecuerExecThreads;
    }

    public int getShellExecThreads() {
        return shellExecThreads;
    }

    public void setShellExecThreads(int shellExecThreads) {
        this.shellExecThreads = shellExecThreads;
    }
}