# Airflow Email Notification Fix

## SMTP Background  
- **SMTP (Simple Mail Transfer Protocol)** is the standard protocol used to send email messages between clients (like Airflow) and mail servers.  
- It defines how messages are routed, delivered, and relayed, and supports both plain-text and encrypted connections.  
- Common ports:  
  - **25** – legacy SMTP (often blocked by ISPs)  
  - **587** – message submission with STARTTLS (recommended for authenticated clients)  
  - **465** – implicit SSL/TLS (legacy SMTPS)

## Issue  
- After a port change, no one on the team was receiving Airflow email alerts.  
- The MWAA environment was still trying to connect on SMTP port 25, which the mail relay no longer accepted.

## Manual Resolution  
1. **Update MWAA Configuration Overrides**  
   - Go to **AWS Console → MWAA → Environments → [your-env] → Edit**  
   - Under **Airflow configuration options**, add:  
     ```text
     Section: smtp  
     Key:     smtp_port  
     Value:   587
     ```  
2. **Redeploy the Environment**  
   - Save changes and allow MWAA to finish its update/restart.
# Version-Controlled SMTP Port Override for MWAA via CDK

## Why Modify via CDK (Not the UI)  
- **No UI Permissions**: You don’t have rights to change Airflow config in the MWAA console.  
- **Version Control**: All infra changes live in code and Git history—no hidden manual tweaks.

## What Is CDK?  
The AWS Cloud Development Kit (CDK) is an open-source software development framework for defining cloud infrastructure as code (IaC). Instead of writing raw CloudFormation JSON/YAML, you use familiar programming languages to model and provision AWS resources.

---

## Key Concepts

- **App**  
  The root of your CDK program. Defines one or more Stacks.
- **Stack**  
  A unit of deployment, corresponding to one CloudFormation stack.
- **Construct**  
  The basic building block. Represents one or more AWS resources (e.g. an S3 bucket, VPC, or even higher-level patterns).
- **Context**  
  Configuration values (from `cdk.json`, CLI flags, or environment) injected into your code at synth/deploy time.
  
# MWAA/CDK `cdk.json`

```json
{
  "app": "npx ts-node bin/your-cdk-app.ts",
  "context": {
    "mwaa-edwh-dbt-2.4.3": {
      "airflow_version": "2.4.3",
      "airflow_iam_policy_name": "AmazonMWAAManagedPolicyDWHDBT",
      "airflow_iam_role_name": "AmazonMWAA-DataPlatform-Airflow-dwh-dbt",
      "airflow_iam_stack_name": "dataplatform-mwaa-iam-role-stack-dwh-dbt",
      "airflow_env_stack_name": "dataplatform-mwaa-stack-dwh-dbt",
      "airflow_env_name": "dwh-dbt",
      "bdc:project": "dwh",
      "dag_s3_path": "airflow/dags",
      "execution_role_arn": "arn:aws:iam::12345678:role/service-role/AmazonMWAA",
      "environment_class": "mw1.large",
      "max_workers": 15,
      "plugins_s3_path": "airflow/plugins/2.4.3/plugins.zip",
      "requirements_s3_path": "airflow/requirements/2.4.3/requirements.txt",
      "scheduler_count": 2,
      "secrets_prefix": "dwh-dbt",
      "options": {
        "webserver.instance_name": "DWH-DBT - prod",
        "webserver.warn_deployment_exposure": false,
        "core.dag_run_conf_overrides_params": true,
        "smtp.smtp_port": 587
      }
    }
  }
}
```
---
## How CDK Works with MWAA  
1. **Context in `cdk.json`**  -> this is going to be an MR that is submitted to the repository to ensure version control 
   You centralize environment settings under a context key (e.g. `"mwaa-dwh-dbt"`), including your SMTP override:  
   ```jsonc
   "context": {
     "mwaa-dwh-dbt": {
       …
       "options": {
         "smtp.smtp_port": 587
       }
     }
   }
2.  Load Context in CDK App
In your CDK entrypoint (e.g. `bin/your-cdk-app.ts`), you retrieve the context values:
```
const app = new cdk.App();
const cfg = app.node.tryGetContext('mwaa-edwh-dbt-2.4.3');
// cfg.options ⇒ { "smtp.smtp_port": 587, … }
```
3. The airflowConfigurationOptions map (from cfg.options) directly translates into the MWAA Airflow configuration overrides. For example::
```
"airflowConfigurationOptions": {
  "smtp.smtp_port":     "587",
  "core.dag_run_conf_overrides_params": "true",
  "webserver.instance_name":            "DWH-DBT - prod",
  "webserver.warn_deployment_exposure": "false"
}
```
4. Synthesize:
Generates the CloudFormation template with your MWAA environment and IAM resources:
```
cdk synth mwaa-edwh-dbt-2.4.3
```
5. Deploy:
Provisions/updates the IAM stacks and the MWAA environment. Behind the scenes, MWAA picks up your DAGs/plugins/requirements from S3 and applies the overridden settings (including your SMTP port change).
```
cdk deploy mwaa-edwh-dbt-2.4.3
```

## Outcome  
- Context in cdk.json centralizes all MWAA settings.
- CDK constructs convert those settings into CloudFormation resources: IAM roles, policies, and the MWAA CfnEnvironment.
- Airflow config overrides (options) map directly to MWAA’s “Airflow configuration options.”
- A simple cdk deploy handles the full lifecycle: build IAM, configure MWAA, upload assets, and restart the environment with your new settings.
- Airflow now submits SMTP connections on port 587 (with STARTTLS) instead of port 25.  
- Email alerts are being delivered successfully.
