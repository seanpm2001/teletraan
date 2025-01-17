/**
 * Copyright 2016 Pinterest, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *    
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.teletraan.worker;

import com.pinterest.deployservice.dao.BuildDAO;
import com.pinterest.deployservice.dao.UtilDAO;
import com.pinterest.deployservice.metrics.MeterConstants;
import com.pinterest.teletraan.TeletraanServiceContext;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;

/**
 * Remove unused and old builds.
 * <p>
 * If a build is old enough and unused by a deploy,
 * remove it from build tables.
 * And we need to keep the number of builds is not more than maxToKeep
 */
public class BuildJanitor implements Job {
    private static final Logger LOG = LoggerFactory.getLogger(BuildJanitor.class);
    private static final long MILLIS_PER_DAY = 86400000;
    private Counter errorBudgetSuccess;
    private Counter errorBudgetFailure;

    public BuildJanitor() {
        // If using the Job interface, must keep constructor empty.
    }

    void processBuilds(TeletraanServiceContext workerContext) throws Exception {
        BuildDAO buildDAO = workerContext.getBuildDAO();
        UtilDAO utilDAO = workerContext.getUtilDAO();
        long timeToKeep = (long) workerContext.getMaxDaysToKeep() * MILLIS_PER_DAY;
        long maxToKeep = (long) workerContext.getMaxBuildsToKeep();
        long timeThreshold = System.currentTimeMillis() - timeToKeep;

        List<String> buildNames = buildDAO.getAllBuildNames();
        Collections.shuffle(buildNames);

        for (String buildName : buildNames) {
            long numToDelete = buildDAO.countBuildsByName(buildName) - maxToKeep;
            if (numToDelete > 0) {
                String buildLockName = String.format("BUILDJANITOR-%s", buildName);
                Connection connection = utilDAO.getLock(buildLockName);
                if (connection != null) {
                    LOG.info(String.format("DB lock operation is successful: get lock %s", buildLockName));
                    try {
                        buildDAO.deleteUnusedBuilds(buildName, timeThreshold, numToDelete);
                        LOG.info(String.format("Successfully removed builds: %s before %d milliseconds has %d.",                            buildName, timeThreshold, numToDelete));
                    } catch (Exception e) {
                        LOG.error("Failed to delete builds from tables.", e);

                        errorBudgetFailure.increment();

                    } finally {
                        utilDAO.releaseLock(buildLockName, connection);
                        LOG.info(String.format("DB lock operation is successful: release lock %s", buildLockName));
                    }
                } else {
                    LOG.warn(String.format("DB lock operation fails: failed to get lock %s", buildLockName));
                }
            }
        }

    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            LOG.info("Start build janitor process...");
            SchedulerContext schedulerContext = context.getScheduler().getContext();
            TeletraanServiceContext workerContext = (TeletraanServiceContext) schedulerContext.get("serviceContext");
            
            errorBudgetSuccess = Metrics.counter(MeterConstants.ERROR_BUDGET_METRIC_NAME,
                MeterConstants.ERROR_BUDGET_TAG_NAME_RESPONSE_TYPE, MeterConstants.ERROR_BUDGET_TAG_VALUE_RESPONSE_TYPE_SUCCESS,
                MeterConstants.ERROR_BUDGET_TAG_NAME_METHOD_NAME, this.getClass().getSimpleName());
                
            errorBudgetFailure = Metrics.counter(MeterConstants.ERROR_BUDGET_METRIC_NAME,
                MeterConstants.ERROR_BUDGET_TAG_NAME_RESPONSE_TYPE, MeterConstants.ERROR_BUDGET_TAG_VALUE_RESPONSE_TYPE_FAILURE,
                MeterConstants.ERROR_BUDGET_TAG_NAME_METHOD_NAME, this.getClass().getSimpleName());

            processBuilds(workerContext);
            LOG.info("Stop build janitor process...");

            errorBudgetSuccess.increment();
        } catch (Throwable t) {
            LOG.error("Failed to call build janitor.", t);

            if(errorBudgetFailure != null)
                errorBudgetFailure.increment();
        }
    }
}
