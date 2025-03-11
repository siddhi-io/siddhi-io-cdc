/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.io.cdc.source.polling;

import io.siddhi.extension.io.cdc.source.config.CronConfiguration;
import io.siddhi.extension.io.cdc.source.polling.strategies.PollingStrategy;
import io.siddhi.extension.io.cdc.util.CDCSourceConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

/**
 * CDCCronExecutor is executed when the cron expression is given. If the current time satisfied by the cron
 * expression then it will print the inserts and updates happened after App get started.
 */
public class CDCCronExecutor implements Job {
    private static final Logger log = LogManager.getLogger(CDCCronExecutor.class);

    public CDCCronExecutor() {
    }

    /**
     * To initialize the cron job to execute at given cron expression
     */
    public static void scheduleJob(PollingStrategy pollingStrategy, CronConfiguration cronConfiguration) {
        try {
            String cronExpression = cronConfiguration.getCronExpression();
            JobKey jobKey = new JobKey(CDCSourceConstants.JOB_NAME, CDCSourceConstants.JOB_GROUP);
            Scheduler scheduler = new StdSchedulerFactory().getScheduler();
            if (scheduler.checkExists(jobKey)) {
                scheduler.deleteJob(jobKey);
            }
            scheduler.start();
            cronConfiguration.setScheduler(scheduler);
            // JobDataMap used to access the object in the job class
            JobDataMap dataMap = new JobDataMap();
            dataMap.put(CDCSourceConstants.POLLING_STRATEGY, pollingStrategy);

            // Define instances of Jobs
            JobDetail cron = JobBuilder.newJob(CDCCronExecutor.class)
                    .usingJobData(dataMap)
                    .withIdentity(jobKey)
                    .build();
            //Trigger the job to at given cron expression
            Trigger trigger = TriggerBuilder
                    .newTrigger()
                    .withIdentity(CDCSourceConstants.TRIGGER_NAME, CDCSourceConstants.TRIGGER_GROUP)
                    .withSchedule(
                            CronScheduleBuilder.cronSchedule(cronExpression)).build();
            // Tell quartz to schedule the job using our trigger
            scheduler.scheduleJob(cron, trigger);
        } catch (SchedulerException e) {
            log.error("An error occurred when scheduling job in siddhi app for cdc polling {}", e, e);
        }
    }

    /**
     * Method gets called when the cron Expression satisfies the system time
     */
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        PollingStrategy pollingStrategy = (PollingStrategy) dataMap.get(CDCSourceConstants.POLLING_STRATEGY);
        pollingStrategy.poll();
    }
}
