#!/bin/bash

BASE_HDFS_DIRECTORY="your_path"
table_name=tablenames.txt
if [ ! -f $table_name ]; then
    > $table_name
fi

STORAGE_LAYERS=($(hdfs dfs -ls "$BASE_HDFS_DIRECTORY" 2>/dev/null | awk '{print $NF}' | awk -F '/' '{print $NF}'))
if [[ ${#STORAGE_LAYERS[@]} -eq 0 ]]; then
    echo "No storage layers found in HDFS. Exiting."
    exit 1
fi

echo "Available Storage Layers:"
for i in "${!STORAGE_LAYERS[@]}"; do
    echo "$((i+1)). ${STORAGE_LAYERS[$i]}"
done

read -p "Enter your choice (1-${#STORAGE_LAYERS[@]}): " choice
if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#STORAGE_LAYERS[@]} )); then
    STORAGE_LAYER="${STORAGE_LAYERS[$((choice-1))]}"
else
    echo "Invalid selection. Exiting."
    exit 1
fi
clear || tput reset
echo "Selected Storage Layer: $STORAGE_LAYER"

FILE_DIRECTORIES=($(hdfs dfs -ls "$BASE_HDFS_DIRECTORY/$STORAGE_LAYER" 2>/dev/null | awk '{print $NF}' | awk -F '/' '{print $NF}'))

if [[ ${#FILE_DIRECTORIES[@]} -eq 0 ]]; then
    echo "No file directories found under $STORAGE_LAYER. Exiting."
    exit 1
fi

echo "Available File Directories:"
for i in "${!FILE_DIRECTORIES[@]}"; do
    echo "$((i+1)). ${FILE_DIRECTORIES[$i]}"
done

read -p "Enter your choice (1-${#FILE_DIRECTORIES[@]}): " choice
if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#FILE_DIRECTORIES[@]} )); then
    FILE_LOCATION="${FILE_DIRECTORIES[$((choice-1))]}"
else
    echo "Invalid selection. Exiting."
    exit 1
fi
clear || tput reset
echo "Selected File Directory: $FILE_LOCATION"

HIVE_OUTPUT=$(hive -e "SHOW DATABASES;" 2>/dev/null)
CLEANED_OUTPUT=$(echo "$HIVE_OUTPUT" | awk 'NF >= 3 {print $2}' | sed '1d')
DATABASES=($CLEANED_OUTPUT)

if [[ ${#DATABASES[@]} -eq 0 ]]; then
    echo "No Hive databases found. Exiting."
    exit 1
fi

echo "Available Databases:"
for i in "${!DATABASES[@]}"; do
    echo "$((i+1)). ${DATABASES[$i]}"
done

read -p "Enter your choice (1-${#DATABASES[@]}): " choice
if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#DATABASES[@]} )); then
    DB_NAME="${DATABASES[$((choice-1))]}"
else
    echo "Invalid selection. Exiting."
    exit 1
fi
clear || tput reset
echo "Selected Database: $DB_NAME"

read -p "Enter Table Name: " TBL_NAME
echo -e "prod_sdm.$DB_NAME.$TBL_NAME" >> "$table_name"

HDFS_LOCATION="$BASE_HDFS_DIRECTORY/$STORAGE_LAYER/$FILE_LOCATION"

echo -n "Table Creating..."
sleep 1
echo -n "Wait few seconds!"
#######################################################################################################################################################################################################################

LATEST_PARQUET_FILE=$(hdfs dfs -ls -R "$HDFS_LOCATION" | awk '{print $NF}' | grep -i "\.parquet$" | head -n 1)
if [[ -z "$LATEST_PARQUET_FILE" ]]; then
    echo "Error: No Parquet file found in $HDFS_LOCATION"
    exit 1
fi

LOCAL_PARQUET_FILE="temp.parquet"
hdfs dfs -get "$LATEST_PARQUET_FILE" "$LOCAL_PARQUET_FILE"

PARTITIONS=$(echo "$LATEST_PARQUET_FILE" | awk -F'/' '{for(i=1;i<=NF;i++) if($i ~ /=/) print substr($i, 1, index($i, "=")-1)}' | tr '\n' ',' )
PARTITIONS=${PARTITIONS%,} 

PARTITION_COLUMNS=$(echo "$PARTITIONS"  | awk -F',' '{for(i=1;i<=NF;i++) print "\`"$i"\` STRING,"}' | tr -d '\n') 
PARTITION_COLUMNS=${PARTITION_COLUMNS%,}

SCHEMA=$(parquet-tools schema "$LOCAL_PARQUET_FILE" | awk 'NF >= 3 {print $2, $3}' | sed '1d')
SCHEMA=$(echo "$SCHEMA" | tr -d ';')

file=columns_types.txt
> $file

echo "$SCHEMA" | while IFS= read -r line; do
    #COLUMN_NAME=$(echo "$line" | awk '{print $2}')
    COLUMN_NAME="\`$(echo "$line" | awk '{print $2}')\`"
    COLUMN_TYPE=$(echo "$line" | awk '{print $1}')

    case "$COLUMN_TYPE" in
        binary) SQL_TYPE="STRING" ;;
        int64) SQL_TYPE="BIGINT" ;;
        int32) SQL_TYPE="INT" ;;
        int96) SQL_TYPE="TIMESTAMP" ;;
        double) SQL_TYPE="DOUBLE" ;;
        float) SQL_TYPE="FLOAT" ;;
        boolean) SQL_TYPE="BOOLEAN" ;;
        fixed_len_byte_array*) SQL_TYPE="DECIMAL(38,6)" ;;
        varchar) SQL_TYPE="STRING" ;;
        decimal*) SQL_TYPE="DECIMAL(30,10)" ;;
        timestamp*) SQL_TYPE="TIMESTAMP" ;;
        date) SQL_TYPE="DATE" ;;
        *) SQL_TYPE="STRING" ;;  # Default fallback
    esac
    echo -e "$COLUMN_NAME $SQL_TYPE," >> "$file"
done

query_structure=$(cat $file | sed '${s/,$//}')
SQL_QUERY="CREATE EXTERNAL TABLE IF NOT EXISTS $DB_NAME.$TBL_NAME (
$query_structure
)
partitioned by ($PARTITION_COLUMNS)
STORED AS PARQUET
LOCATION 'hdfs://cemprod$HDFS_LOCATION';"

hive -e "$SQL_QUERY" 2>/dev/null

if [ $? -eq 0 ]; then
    hive -e "MSCK REPAIR TABLE $DB_NAME.$TBL_NAME;" 2>/dev/null
    kill $waiting >/dev/null 2>&1
    wait $waiting 2>/dev/null
    clear || tput reset
    echo "==============================================================================="
    echo "âœ… Selected Storage Layer: $STORAGE_LAYER"
    echo "âœ… Selected File Directory: $FILE_LOCATION"
    echo "âœ… Selected Database: $DB_NAME"
    echo "âœ… Table Name: $TBL_NAME"
    echo "âœ… HDFS Location: $HDFS_LOCATION"
    echo "âœ… Status: Created! & Repaired"
    echo "==============================================================================="
else
    echo "Error: Failed to create table in Hive."
    kill $waiting >/dev/null 2>&1
    wait $waiting 2>/dev/null
    exit 1
fi


rm $file
rm $LOCAL_PARQUET_FILE

while true; do
    read -p "Do you wanna create another External Table (Y/N): " user
    if [[ "$user" = "Y" ]]; then
         clear || tput reset
         sh your_script_path/external_table_creation.sh
    else
	clear || tput reset
	echo "-------------------------------------------------------------------------------------------"
	echo "List of tables created:"
        count=1
	while IFS= read -r line; do
            echo "$count. $line"
            ((count++))
        done < $table_name
        echo ""
        echo "Thanks for Using!"
        rm $table_name
	break
    fi
done 

#######################################################################################################################################################################################################################
