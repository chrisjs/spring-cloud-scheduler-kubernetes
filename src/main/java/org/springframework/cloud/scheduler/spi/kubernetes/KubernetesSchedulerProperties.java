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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the Kubernetes Scheduler.
 *
 * @author Chris Schaefer
 */
@ConfigurationProperties(prefix = KubernetesSchedulerProperties.KUBERNETES_SCHEDULER_PROPERTIES)
public class KubernetesSchedulerProperties {
	public static final String KUBERNETES_SCHEDULER_PROPERTIES = "spring.cloud.scheduler.kubernetes";

	private static String KUBERNETES_NAMESPACE = System.getenv("KUBERNETES_NAMESPACE") != null
			? System.getenv("KUBERNETES_NAMESPACE")
			: "default";

	private ImagePullPolicy imagePullPolicy = ImagePullPolicy.IfNotPresent;

	private RestartPolicy restartPolicy = RestartPolicy.Never;

	private String namespace = KUBERNETES_NAMESPACE;

	public ImagePullPolicy getImagePullPolicy() {
		return imagePullPolicy;
	}

	public void setImagePullPolicy(ImagePullPolicy imagePullPolicy) {
		this.imagePullPolicy = imagePullPolicy;
	}

	public RestartPolicy getRestartPolicy() {
		return restartPolicy;
	}

	public void setRestartPolicy(RestartPolicy restartPolicy) {
		this.restartPolicy = restartPolicy;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
}
