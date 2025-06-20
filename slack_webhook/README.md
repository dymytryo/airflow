# Airflow Slack Alert Integration

A practical, production-style solution for integrating Slack-based alerting into Airflow DAGs—supports multi-channel notifications, reusability, proxy configs, and email fallback.

---

## 📣 Overview

This project demonstrates:

- **How to set up Slack Incoming Webhooks for Airflow**
- **Reusable, documented alert utilities for task failure, warning, and success**
- **Sending alerts to multiple Slack channels per task/DAG**
- **Optional email alert integration**
- **Best practices for cloud/VPC/proxy environments**

---

## 🚦 How It Works

```mermaid
flowchart TD
    A[Airflow Task Fails]
    B{on_failure_callback triggered}
    C1[Send alert to Slack Channel 1]
    C2[Send alert to Slack Channel 2]
    D[Email alert sent (if configured)]
    E[Alert seen by Data Team]
    A --> B
    B --> C1
    B --> C2
    B --> D
    C1 --> E
    C2 --> E
    D --> E
```

- **On task failure** (or retry/success), Airflow executes a Python callback that pushes a message to one or more Slack channels (and optionally, emails).
- Slack connections and channels are configured as Airflow HTTP Connections for secure, environment-specific routing.

---

## 🛠️ Setup Instructions

### 1. Create Slack Webhook

1. Go to [https://api.slack.com/apps](https://api.slack.com/apps)
2. Press `Create New App` → `From Scratch`
3. On the left, in **Features** → **Incoming Webhooks**
4. **Activate Incoming Webhooks** → **On**
5. **Add New Webhook** (choose your target channel)
6. Example output:

| Webhook URL                                                 | Channel      | Added By           | Date Added    |
|-------------------------------------------------------------|--------------|--------------------|--------------|
| `https://hooks.slack.com/services/TXXXXX/FDJSXXXXXX/sdfdXXXXXXXXXX` | #dbt-alerts  | Your Name           | Jun 19, 2025 |

7. **Test with cURL:**
   ```shell
   curl -X POST -H 'Content-type: application/json' --data '{"text":"Testing Slack webhook!"}' https://hooks.slack.com/services/TXXXXX/FDJSXXXXXX/sdfdXXXXXXXXXX
   ```

---

### 2. Register Slack Webhook in Airflow

1. Go to **Admin → Connections → +**
2. Enter details:

| Config Field      | Value                                       |
|-------------------|---------------------------------------------|
| Connection Id     | `slack_webhook_dbt_alerts`                  |
| Connection Type   | `HTTP`                                      |
| Host              | `https://hooks.slack.com/services/`         |
| Password          | `TXXXXX/FDJSXXXXXX/sdfdXXXXXXXXXX`          |
| Extra             | `{"proxy": "http://proxy.local:3128"}` (optional) |

**Repeat for each channel you wish to support.**

---

### 3. Integrate with Your DAG

- Import the alert utility:
  ```python
  from utilities.slack_operator import task_fail_slack_alert
  ```

- Example callback for multi-channel Slack alerts:
  ```python
  def dag_fail_alerts(context):
      task_fail_slack_alert(context, "slack_webhook_dbt_alerts")
      task_fail_slack_alert(context, "slack_webhook_secondary")
  ```

- Add to your DAG’s `default_args`:
  ```python
  default_args = {
      # ... other args ...
      "on_failure_callback": dag_fail_alerts,
  }
  ```

---

## 🧑‍💻 Example Project Structure

```
.
├── dags/
│   └── example_dag.py
├── utilities/
│   └── slack_operator.py
├── README.md
```

---

## 💡 Features

- **Multiple Slack channels per alert:** Simply call the utility for each connection.
- **Task, warning, and success alerts:** Choose which events to notify.
- **Proxy/VPC ready:** Supports Airflow's HTTP Connection `Extra` for proxies.
- **Email fallback:** Configure as needed, or silence email if SMTP is not used.

---

## 📢 Portfolio Example

> Implemented robust, multi-channel Slack alerting for Airflow, enabling rapid failure detection for dbt and vendor pipelines, with proxy/cloud support and modular utilities for large-scale data engineering teams.

---

**Ready to deploy!  
Questions or improvements? Feel free to fork and adapt!**

