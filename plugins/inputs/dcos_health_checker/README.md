# Dcos health Input Plugin

### Configuration:

```toml
[[inputs.dcos_health_checker]]
  masters = ["10.168.0.80"]
  cluster_path = "/etc/cluster-node"
  master_health_port = "5050"
  slave_health_port = "5051"
  units_health_port = "80"
  gather_node_health = true
  gather_node_units_health = true

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false

  ## Amount of time allowed to complete the Gather request
  # timeout = "5s"
```