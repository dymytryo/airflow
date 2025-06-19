# Create Slack Webhook 

1. Go to `https://api.slack.com/apps`
2. Press `Create New App` >> `From Scratch`
3. On the left, in `Features` >> `Incoming Webhooks`
4. `Activate Incoming Webhooks` >> `On`
5. `Add New Webhook`
6. This is what the created webhook would look like:

| Webhook URL                            | Channel            | Added By         | Date Added    |
|-----------------------------------------|--------------------|------------------|--------------|
| `https://hooks.slack.com/services/TXXXXX/FDJSXXXXXX/sdfdXXXXXXXXXX`  | #dbt-alerts   | Dmytro Valiaiev  | Jun 19, 2025 |

7. Test it with a command in local terminal: 
```shell
curl -X POST -H 'Content-type: application/json' --data '{"text":"Dmytro testing!"}' https://hooks.slack.com/services/TXXXXX/FDJSXXXXXX/sdfdXXXXXXXXXX
```

# In Airflow

1. `Admin` >> `Connections` >> `+`
2. Separate the link that Slack produces into Host and Password variables: 

| Config                         | Value            |
|-----------------------------------------|--------------------|
| `Connection Id`   | `slack_webhook_dbt_alerts`   |
| `Connection Type ` | `HTTP` |
| `Host` | `https://hooks.slack.com/services/`|
| `Password` | `TXXXXX/FDJSXXXXXX/sdfdXXXXXXXXXX` |
| `Extra` | `{"proxy": "http://proxy.local:3128"}` |

