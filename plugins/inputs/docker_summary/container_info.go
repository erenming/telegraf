package dockersummary

import (
	"log"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/influxdata/telegraf"
)

func (s *Summary) gatherContainer(id string, c *types.Container, acc telegraf.Accumulator) error {
	info, err := s.dockerInspect(id)
	if err != nil {
		return err
	}

	tags := make(map[string]string)
	fields := make(map[string]interface{})

	if c != nil {
		// Parse container name
		var cname string
		for _, name := range c.Names {
			cname = strings.TrimPrefix(name, "/")
		}
		if cname == "" {
			return nil
		}
		tags["container_name"] = cname
	}
	if info.Name != "" {
		tags["container_name"] = strings.TrimPrefix(info.Name, "/")
	}

	// the image name sometimes has a version part, or a private repo
	//   ie, rabbitmq:3-management or docker.someco.net:4443/rabbitmq:3-management
	imageName, imageVersion := s.getDockerImage(info, c)
	tags["container_image"] = imageName
	tags["image_version"] = imageVersion
	tags["container_id"] = id

	// Add labels to tags
	if len(s.LabelInclude) > 0 && info.Config != nil {
		for k, label := range info.Config.Labels {
			if s.labelFilter.Match(k) {
				tags[k] = label
			}
		}
	}
	if info.Config != nil {
		// 标记 k8s pause 容器
		if info.Config.Labels[labelKubernetesContainerName] == "POD" &&
			info.Config.Labels[labelKubernetesType] == "podsandbox" {
			tags["podsandbox"] = "true"
		}
	}

	// env to map
	envs := make(map[string]string, len(info.Config.Env))
	for _, env := range info.Config.Env {
		kvs := strings.SplitN(env, "=", 2)
		if len(kvs) == 2 && len(strings.TrimSpace(kvs[1])) != 0 {
			envs[kvs[0]] = strings.TrimSpace(kvs[1])
		}
	}
	// Add whitelisted environment variables to tags
	if len(s.EnvInclude) > 0 {
		for key, val := range envs {
			if s.envFilter.Match(key) {
				tags[key] = val
			}
		}
	}

	tm, err := s.gatherContainerStats(id, tags, fields, envs, info, acc)
	if err != nil {
		log.Println(err)
	}

	acc.AddFields("docker_container_summary", fields, tags, tm)
	return nil
}

func (s *Summary) getDockerImage(info *types.ContainerJSON, c *types.Container) (string, string) {
	var image string
	if c != nil {
		image = c.Image
	} else {
		image = info.Image
	}
	if strings.HasPrefix(image, "sha") || strings.Contains(image, "@sha256:") {
		inspect, err := s.dockerImageInspect(image)
		if err != nil {
			log.Printf("W! failed to get docker image, id: %s, error: %s", image, err)
		} else if len(inspect.RepoTags) == 0 {
			image = inspect.Config.Image
		} else {
			image = inspect.RepoTags[0]
		}
	}
	// the image name sometimes has a version part, or a private repo
	//   ie, rabbitmq:3-management or docker.someco.net:4443/rabbitmq:3-management
	imageName, imageVersion := "", "unknown"
	i := strings.LastIndex(image, ":") // index of last ':' character
	if i > -1 {
		imageVersion = image[i+1:]
		imageName = image[:i]
	} else {
		imageName = image
	}
	return imageName, imageVersion
}
