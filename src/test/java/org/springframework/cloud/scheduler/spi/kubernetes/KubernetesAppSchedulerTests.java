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

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.deployer.resource.docker.DockerResource;
import org.springframework.cloud.scheduler.spi.core.Scheduler;
import org.springframework.cloud.scheduler.spi.core.SchedulerPropertyKeys;
import org.springframework.cloud.scheduler.spi.test.AbstractIntegrationTests;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.NONE;

/**
 * KubernetesAppScheduler tests
 *
 * @author Chris Schaefer
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = NONE)
@ContextConfiguration(classes = { KubernetesAppSchedulerTests.Config.class })
public class KubernetesAppSchedulerTests extends AbstractIntegrationTests {
	@Autowired
	private Scheduler scheduler;

	@Override
	protected Scheduler provideScheduler() {
		return this.scheduler;
	}

	@Override
	protected List<String> getCommandLineArgs() {
		return null;
	}

	@Override
	protected Map<String, String> getSchedulerProperties() {
		return Collections.singletonMap(SchedulerPropertyKeys.CRON_EXPRESSION, "8 16 ? * *");
	}

	@Override
	protected Map<String, String> getDeploymentProperties() {
		return null;
	}

	@Override
	protected Map<String, String> getAppProperties() {
		return null;
	}

	@Override
	// schedule name must match "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" and size must be between 0
	// and 63
	protected String randomName() {
		return UUID.randomUUID().toString().substring(0, 18);
	}

	@Override
	// schedule name must match "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" and size must be between 0
	// and 63
	protected String scheduleName() {
		return "schedulename-";
	}

	protected Resource testApplication() {
		return new DockerResource("springcloud/spring-cloud-scheduler-spi-test-app:latest");
	}

	@Configuration
	@EnableAutoConfiguration
	@EnableConfigurationProperties
	public static class Config {
		private KubernetesSchedulerProperties kubernetesSchedulerProperties = new KubernetesSchedulerProperties();

		@Bean
		public Scheduler scheduler() {
			io.fabric8.kubernetes.client.Config config = io.fabric8.kubernetes.client.Config.autoConfigure(null);
			config.setNamespace(kubernetesSchedulerProperties.getNamespace());

			return new KubernetesAppScheduler(new DefaultKubernetesClient(config), kubernetesSchedulerProperties);
		}
	}
}
