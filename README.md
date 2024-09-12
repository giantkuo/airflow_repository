# Airflow DAGs Repository

本儲存庫包含了在 Kubernetes 上使用 Helm 部署的 Airflow 中所有的 DAGs (Directed Acyclic Graphs)。這些 DAGs 用於管理和自動化資料工作流程。儲存庫僅用來存放 DAGs，Airflow 環境則是在 Kubernetes 上運行。

## 專案結構

- `dags/`：儲存庫中的主要目錄，存放所有 Airflow DAG 文件。
- 其他目錄或文件會根據需求添加。

## 安裝與配置

儘管此儲存庫僅包含 DAGs，以下步驟簡要介紹如何在 Kubernetes 上使用 Helm 安裝 Airflow 並配置 DAGs。

1. **安裝 Airflow on Kubernetes**:

   使用 Helm chart 安裝 Airflow：

   ```
   helm repo add apache-airflow https://airflow.apache.org
   helm install airflow apache-airflow/airflow
   ```

   這將在你的 Kubernetes 叢集中部署一個完整的 Airflow 環境。

2. **部署 DAGs**：
   將本儲存庫中的 dags/ 目錄下的所有 DAGs 上傳到 Airflow 節點的對應 DAG 資料夾中。例如，使用以下指令將 DAGs 上傳至 Airflow：
   ```
   kubectl cp dags/ <your-airflow-pod-name>:/opt/airflow/dags/
   ```
   DAGs 上傳後，Airflow 會自動檢測並載入這些 DAGs。

DAGs 文件

本儲存庫中的 DAGs 各自負責不同的工作流程。以下是每個 DAG 的簡要說明：

1. email_notification_Allen.py
   此 Airflow DAG 用於自動檢查擁有者為Allen的遠端 Raspberry Pi 相機的文件狀態，並通過電子郵件發送報告。它每天自動運行一次，確保正確檢測相機資料夾中的文件是否存在。
   
2. email_notification_AndyCW.py
   此 Airflow DAG 用於自動檢查擁有者為AndyCW的遠端 Raspberry Pi 相機的文件狀態，並通過電子郵件發送報告。它每天自動運行一次，確保正確檢測相機資料夾中的文件是否存在。
