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

## Resolution  
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

## Outcome  
- Airflow now submits SMTP connections on port 587 (with STARTTLS) instead of port 25.  
- Email alerts are being delivered successfully.
