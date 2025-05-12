# HDFS External Table Creation Tool

This shell based tool simplifies creation of external Hive tables on top of Parquet datasets stored in HDFS. It automates schema extraction, partition detection, DDL generation, and partition repair for Hive tables.

## ✨ Features

- Automatically detects schema from latest parquet file
- Extracts partition information
- Generates Hive DDL automatically
- Repairs table partitions post creation
- Interactive and easy to use

## 📁 Directory Structure

- script/ # external_table_creation.sh
- docs/ # documentation and screenshots
- examples/ # sample output files

## ⚙️ Prerequisites

Ensure the following tools are available on your system:

- `bash`
- `hdfs dfs` (Hadoop CLI)
- `hive` CLI
- `parquet-tools`

## 🛠️ Deployment in Your Environment

To deploy and use this script on your own system:

1. **Download script from repository**

  external_table_creation.sh

2. **Update base HDFS directory path.**

   Open `create_external_table.sh` and modify following line at top of script:

   ```bash
   BASE_HDFS_DIRECTORY="your_path"

3. **Update script path (in main external_table_creation.sh) for recursive call**

Open `create_external_table.sh` and modify following line at botton of script where you will find the recursive call:

sh your_script_location/external_table_creation.sh

## **Yaa Hoo! now use this tool to create the external table, no need to manually inspect hdfs or write hive queries.**

4. **🚀 Usage**

Follow the steps below to create the external table or download the guide document along with proper screenshots from the repository for easier use of this tool.

1. Run the main script:
   ```bash
   sh create_external_table.sh

2. Follow interactive steps:

- Select storage layer
- Choose Parquet file directory
- Pick Hive database
- Provide table name
- Tool will auto-detect schema and create external table

3. After creation:

Partitions will be repaired automatically
You will be prompted to create another table or exit


