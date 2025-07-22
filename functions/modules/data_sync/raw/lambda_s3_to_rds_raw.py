import os
import boto3
import psycopg2
import csv
import json
import logging
from urllib.parse import unquote_plus
from datetime import datetime

def lambda_handler(event, context):
    # 日志配置
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # 环境变量配置
    s3_bucket = os.environ.get('S3_BUCKET', 'insightflow-raw-bucket')
    rds_host = os.environ.get('RDS_HOST')
    rds_port = int(os.environ.get('RDS_PORT', '5432'))
    rds_db = os.environ.get('RDS_DB')
    rds_user = os.environ.get('RDS_USER')
    rds_password = os.environ.get('RDS_PASSWORD')
    table_name_env = os.environ.get('TABLE_NAME')
    s3_key_prefix_env = os.environ.get('S3_KEY_PREFIX')  # 多前缀支持，逗号分隔
    start_ts = os.environ.get('START_TIMESTAMP')  # 手动定义需同步数据的起始时间戳
    end_ts = os.environ.get('END_TIMESTAMP')    # 手动定义需同步数据的结束时间戳
    
    if table_name_env:
        try:
            table_names = json.loads(table_name_env) if table_name_env.startswith('[') else [t.strip() for t in table_name_env.split(',')]
            logger.info(f"解析到 table_names: {table_names}")
        except Exception as e:
            logger.error(f"解析 TABLE_NAME 环境变量失败: {e}")
            table_names = [table_name_env]
    else:
        table_names = []
    schema_name = os.environ.get('SCHEMA_NAME', 'insightflow_raw')

    s3 = boto3.client('s3')
    batch_files = {}

    records = event.get('Records', [])
    s3_keys = []
    if 's3_keys' in event:
        s3_keys = event['s3_keys']
        logger.info(f"收到 s3_keys: {s3_keys}")
    elif s3_key_prefix_env:
        # 多前缀支持，逗号分隔，自动映射表名
        s3_key_prefixes = [p.strip() for p in s3_key_prefix_env.split(",") if p.strip()]
        logger.info(f"自动遍历 S3 路径前缀列表: {s3_key_prefixes}")
        prefix_table_map = {}
        for prefix in s3_key_prefixes:
            # 例如 data/batch/aisles/ → aisles
            table_candidate = prefix.rstrip('/').split('/')[-1]
            prefix_table_map[prefix] = table_candidate
        logger.info(f"前缀与表名映射: {prefix_table_map}")
        for s3_key_prefix in s3_key_prefixes:
            continuation_token = None
            while True:
                if continuation_token:
                    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key_prefix, ContinuationToken=continuation_token)
                else:
                    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key_prefix)
                contents = response.get('Contents', [])
                for obj in contents:
                    key = obj['Key']
                    # 只收集 .csv 和 .gz 文件
                    if key.endswith('.csv') or key.endswith('.gz'):
                        # 只注入到对应表
                        table_candidate = prefix_table_map[s3_key_prefix]
                        batch_files.setdefault(table_candidate, []).append(key)
                if response.get('IsTruncated'):
                    continuation_token = response.get('NextContinuationToken')
                else:
                    break
        logger.info(f"自动收集到 batch_files: {batch_files}")
    else:
        logger.info("未收到 s3_keys,也未配置 S3_KEY_PREFIX,使用自定义触发方式")

    def extract_ts_from_key(s3_key):
        year = month = day = hhmm = None
        for part in s3_key.split('/'):
            if part.startswith('year='):
                year = part.split('=')[1]
            elif part.startswith('month='):
                month = part.split('=')[1]
            elif part.startswith('day='):
                day = part.split('=')[1]
            elif part.startswith('hhmm='):
                hhmm = part.split('=')[1]
        if year and month and day and hhmm:
            # 返回标准化字符串和datetime对象
            ts_str = f"{year}-{month}-{day}-{hhmm}"
            try:
                ts_dt = datetime.strptime(ts_str, "%Y-%m-%d-%H%M")
            except Exception:
                ts_dt = None
            return ts_str, year, month, day, hhmm, ts_dt
        return None, year, month, day, hhmm, None

    # 解析环境变量中的时间戳为datetime对象 - 全局应用于所有同步模式
    start_dt = None
    end_dt = None
    if start_ts:
        try:
            start_dt = datetime.strptime(start_ts, "%Y-%m-%d-%H%M")
            logger.info(f"解析 start_ts: {start_ts} 为 {start_dt}")
        except Exception:
            logger.warning(f"无法解析 start_ts: {start_ts}")
    if end_ts:
        try:
            end_dt = datetime.strptime(end_ts, "%Y-%m-%d-%H%M")
            logger.info(f"解析 end_ts: {end_ts} 为 {end_dt}")
        except Exception:
            logger.warning(f"无法解析 end_ts: {end_ts}")
    
    # 按批次分组并过滤时间戳范围（对所有同步模式进行处理）
    if s3_key_prefix_env:
        # 处理自动遍历收集的文件，应用时间戳过滤
        filtered_batch_files = {}
        for table, keys in batch_files.items():
            filtered_keys = []
            for s3_key in keys:
                ts_str, _, _, _, _, ts_dt = extract_ts_from_key(s3_key)
                if start_dt and ts_dt and ts_dt < start_dt:
                    logger.info(f"跳过 {s3_key},ts {ts_str} < start_ts {start_ts}")
                    continue
                if end_dt and ts_dt and ts_dt > end_dt:
                    logger.info(f"跳过 {s3_key},ts {ts_str} > end_ts {end_ts}")
                    continue
                filtered_keys.append(s3_key)
            if filtered_keys:
                filtered_batch_files[table] = filtered_keys
        batch_files = filtered_batch_files
        logger.info(f"时间戳过滤后的 batch_files: {batch_files}")
    elif s3_keys:  # 处理直接提供的 s3_keys
        for s3_key in s3_keys:
            prefix = s3_key.rsplit('_part', 1)[0] if '_part' in s3_key else s3_key
            ts_str, _, _, _, _, ts_dt = extract_ts_from_key(s3_key)
            if start_dt and ts_dt and ts_dt < start_dt:
                logger.info(f"跳过 {s3_key},ts {ts_str} < start_ts {start_ts}")
                continue
            if end_dt and ts_dt and ts_dt > end_dt:
                logger.info(f"跳过 {s3_key},ts {ts_str} > end_ts {end_ts}")
                continue
            filename = os.path.basename(s3_key)
            table_candidate = filename.split('_part')[0].split('.')[0]
            if table_names and table_candidate not in table_names:
                logger.info(f"跳过 {s3_key},表名 {table_candidate} 不在同步列表 {table_names}")
                continue
            batch_files.setdefault(table_candidate, []).append(s3_key)
        logger.info(f"分组后的 batch_files: {batch_files}")

    logger.info(f"分组后的 batch_files: {batch_files}")

    # 连接一次RDS，直接同步所有表（无外键操作）
    try:
        conn = psycopg2.connect(
            host=rds_host, port=rds_port, dbname=rds_db,
            user=rds_user, password=rds_password
        )
        cur = conn.cursor()
        cur.execute(f"SET search_path TO {schema_name};")
        # 测试连接和表访问
        try:
            cur.execute("SELECT 1 FROM insightflow_raw.orders LIMIT 1;")
            logger.info("成功连接并访问 orders 表")
        except Exception as test_error:
            logger.warning(f"测试访问 orders 表失败: {test_error}")
    except Exception as e:
        logger.error(f"连接 RDS 失败: {e}")
        return {'status': 'fail', 'error': str(e)}

    for table, file_keys in batch_files.items():
        logger.info(f"开始处理表 {table},文件数: {len(file_keys)}")
        batch_size = int(os.environ.get('BATCH_SIZE', '10000'))  # 减少批次大小
        for s3_key in sorted(file_keys):
            logger.info(f"处理 S3 文件: {s3_key}")
            _, year, month, day, hhmm, _ = extract_ts_from_key(s3_key)
            try:
                obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
                content = obj['Body'].read().decode('utf-8')
                reader = csv.DictReader(content.splitlines())
                rows = []
                row_count = 0
                for row in reader:
                    row['year'] = year
                    row['month'] = month
                    row['day'] = day
                    row['hhmm'] = hhmm
                    rows.append(row)
                    row_count += 1
                    if len(rows) >= batch_size:
                        # 检查连接状态，如果已断开则重新连接
                        try:
                            if conn.closed or cur.closed:
                                logger.info("检测到连接断开，重新连接数据库")
                                conn = psycopg2.connect(
                                    host=rds_host, port=rds_port, dbname=rds_db,
                                    user=rds_user, password=rds_password
                                )
                                cur = conn.cursor()
                                cur.execute(f"SET search_path TO {schema_name};")
                        except Exception as check_error:
                            logger.warning(f"连接检查失败: {check_error}")
                        
                        success = insert_rows(cur, conn, table, rows, logger)
                        if not success:
                            logger.error(f"插入失败，跳过剩余数据")
                            break
                        logger.info(f"已插入 {len(rows)} 行到 {table}，累计 {row_count} 行")
                        rows = []
                        
                if rows and cur and not cur.closed:
                    success = insert_rows(cur, conn, table, rows, logger)
                    if success:
                        logger.info(f"已插入 {len(rows)} 行到 {table}，文件处理完成，累计 {row_count} 行")
            except Exception as e:
                logger.error(f"处理 S3 文件 {s3_key} 失败: {e}")
                # 检查连接是否还活着，如果断开则重新连接
                try:
                    if conn.closed or cur.closed:
                        logger.info("重新连接数据库")
                        conn = psycopg2.connect(
                            host=rds_host, port=rds_port, dbname=rds_db,
                            user=rds_user, password=rds_password
                        )
                        cur = conn.cursor()
                        cur.execute(f"SET search_path TO {schema_name};")
                except Exception as reconnect_error:
                    logger.error(f"重新连接失败: {reconnect_error}")
                    break

    try:
        if conn and not conn.closed:
            cur.close()
            conn.close()
            logger.info(f"所有表数据同步完成")
    except Exception as e:
        logger.error(f"关闭 RDS 连接失败: {e}")

    return {'status': 'success'}

# 批量插入函数,避免 OOM,增加日志和连接检查
def insert_rows(cur, conn, table_name, rows, logger=None):
    if not rows:
        if logger:
            logger.info(f"insert_rows: 空数据,跳过 {table_name}")
        return True
    
    # 检查连接状态
    try:
        if conn.closed or cur.closed:
            if logger:
                logger.error("数据库连接已关闭，无法插入数据")
            return False
    except Exception as e:
        if logger:
            logger.error(f"检查连接状态失败: {e}")
        return False
    
    columns = rows[0].keys()
    # 处理空值和数据类型转换
    processed_rows = []
    for row in rows:
        processed_row = {}
        for col in columns:
            value = row[col]
            # 处理空值 - 对于数字类型字段，空字符串转换为 NULL
            if value == '' or value is None:
                if col.lower() in ['day_since_prior_order', 'days_since_prior']:
                    processed_row[col] = None
                else:
                    processed_row[col] = value
            else:
                processed_row[col] = value
        processed_rows.append(processed_row)
    
    # 构造 SQL 语句，正确处理 NULL 值
    values_list = []
    for row in processed_rows:
        value_parts = []
        for col in columns:
            value = row[col]
            if value is None:
                value_parts.append('NULL')
            else:
                escaped_value = str(value).replace(chr(39), chr(39)+chr(39))
                value_parts.append(f"'{escaped_value}'")
        values_list.append('(' + ','.join(value_parts) + ')')
    
    values_str = ','.join(values_list)
    # 使用完全限定的表名，避免 search_path 问题
    fully_qualified_table = f"insightflow_raw.{table_name}"
    sql = f"INSERT INTO {fully_qualified_table} ({','.join(columns)}) VALUES {values_str};"
    try:
        cur.execute(sql)
        conn.commit()  # 每次插入后立即提交
        if logger:
            logger.info(f"SQL 执行成功: {sql[:200]}... 共 {len(rows)} 行")
        return True
    except Exception as e:
        if logger:
            logger.error(f"SQL 执行失败: {e}\nSQL: {sql[:500]}...")
        # 只有在连接还活着的情况下才尝试回滚
        try:
            if not conn.closed:
                conn.rollback()  # 回滚失败的事务
                # 重新设置 search_path，防止回滚后丢失
                cur.execute("SET search_path TO insightflow_raw;")
                conn.commit()  # 提交 search_path 设置
                if logger:
                    logger.info("已重新设置 search_path")
            else:
                if logger:
                    logger.warning("连接已关闭，无法回滚")
        except Exception as rollback_error:
            if logger:
                logger.error(f"回滚或重设 search_path 失败: {rollback_error}")
        return False
