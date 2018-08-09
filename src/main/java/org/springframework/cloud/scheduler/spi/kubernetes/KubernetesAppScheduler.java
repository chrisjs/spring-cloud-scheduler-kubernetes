/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.scheduler.spi.kubernetes;

import io.fabric8.kubernetes.api.model.batch.CronJob;
import io.fabric8.kubernetes.api.model.batch.CronJobBuilder;
import io.fabric8.kubernetes.api.model.batch.CronJobList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.springframework.cloud.scheduler.spi.core.CreateScheduleException;
import org.springframework.cloud.scheduler.spi.core.ScheduleInfo;
import org.springframework.cloud.scheduler.spi.core.ScheduleRequest;
import org.springframework.cloud.scheduler.spi.core.Scheduler;
import org.springframework.cloud.scheduler.spi.core.SchedulerException;
import org.springframework.cloud.scheduler.spi.core.SchedulerPropertyKeys;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Kubernetes implementation of the {@link Scheduler} SPI.
 *
 * @author Chris Schaefer
 */
public class KubernetesAppScheduler implements Scheduler {
	// TODO: probably can be removed after fabric8 updates v2alpha1 -> v1beta1
	private static final String CRONJOB_API_VERSION = "batch/v1beta1";

	private static final String SPRING_CRONTAB_KEY = "spring-crontab-id";

	private final KubernetesClient kubernetesClient;

	private final KubernetesSchedulerProperties kubernetesSchedulerProperties;

	public KubernetesAppScheduler(KubernetesClient kubernetesClient,
			KubernetesSchedulerProperties kubernetesSchedulerProperties) {
		this.kubernetesClient = kubernetesClient;
		this.kubernetesSchedulerProperties = kubernetesSchedulerProperties;
	}

	@Override
	public void schedule(ScheduleRequest scheduleRequest) {
		String image = getImage(scheduleRequest);
		Map<String, String> labels = Collections.singletonMap(SPRING_CRONTAB_KEY,
				scheduleRequest.getDefinition().getName());
		String schedule = scheduleRequest.getSchedulerProperties().get(SchedulerPropertyKeys.CRON_EXPRESSION);

		CronJob cronJob = new CronJobBuilder().withNewMetadata().withName(scheduleRequest.getScheduleName())
				.withLabels(labels).endMetadata().withNewSpec().withSchedule(schedule).withNewJobTemplate()
				.withNewSpec().withNewTemplate().withNewSpec().addNewContainer()
				.withName(scheduleRequest.getScheduleName()).withImage(image)
				.withImagePullPolicy(kubernetesSchedulerProperties.getImagePullPolicy().name()).endContainer()
				.withRestartPolicy(kubernetesSchedulerProperties.getRestartPolicy().name()).endSpec().endTemplate()
				.endSpec().endJobTemplate().endSpec().withApiVersion(CRONJOB_API_VERSION).build();

		try {
			kubernetesClient.batch().cronjobs().create(cronJob);
		}
		catch (KubernetesClientException e) {
			throw new CreateScheduleException("Failed to create schedule " + scheduleRequest.getScheduleName(), e);
		}
	}

	@Override
	public void unschedule(String scheduleName) {
		boolean unscheduled = kubernetesClient.batch().cronjobs().withName(scheduleName).delete();

		if (!unscheduled) {
			throw new SchedulerException("Failed to unschedule schedule " + scheduleName + " does not exist.");
		}
	}

	@Override
	public List<ScheduleInfo> list(String taskDefinitionName) {
		return list().stream()
				.filter(cronJob -> taskDefinitionName.equals(cronJob.getTaskDefinitionName()))
				.collect(Collectors.toList());
	}

	@Override
	public List<ScheduleInfo> list() {
		CronJobList cronJobList = kubernetesClient.batch().cronjobs().list();

		List<CronJob> cronJobs = cronJobList.getItems();
		List<ScheduleInfo> scheduleInfos = new ArrayList<>();

		for (CronJob cronJob : cronJobs) {
			ScheduleInfo scheduleInfo = new ScheduleInfo();
			scheduleInfo.setScheduleName(cronJob.getMetadata().getName());
			scheduleInfo.setTaskDefinitionName(cronJob.getMetadata().getLabels().get(SPRING_CRONTAB_KEY));

			Map<String, String> properties = new HashMap<>();
			properties.put(SchedulerPropertyKeys.CRON_EXPRESSION, cronJob.getSpec().getSchedule());
			scheduleInfo.setScheduleProperties(properties);
			scheduleInfos.add(scheduleInfo);
		}

		return scheduleInfos;
	}

	private String getImage(ScheduleRequest scheduleRequest) {
		String image;

		try {
			image = scheduleRequest.getResource().getURI().getSchemeSpecificPart();
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Unable to get URI for " + scheduleRequest.getResource(), e);
		}

		return image;
	}
}
