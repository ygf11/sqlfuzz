import io
import os
import subprocess
from pathlib import Path
from wsgiref import headers

import numpy as np
import pandas as pd
import pytest

pg_db, pg_user, pg_host, pg_port, datafusion_cli = [
    os.environ.get(i)
    for i in (
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "DATAFUSION_CLI"
    )
]

CREATE_TABLE_SQL_FILE = "./testdata/create_table.sql"

def generate_csv_from_datafusion(fname: str):
    return subprocess.run(
        [
            # "./arrow-datafusion/datafusion-cli/target/debug/datafusion-cli",
            # "/home/work/arrow-datafusion/datafusion-cli/target/debug/datafusion-cli",
            datafusion_cli,

            "-f",
            CREATE_TABLE_SQL_FILE,
            "-f",
            fname,
            "--format",
            "csv",
            "-q",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def generate_csv_from_psql(fname: str):

    cmd = ["psql"]

    if pg_db is not None:
        cmd.extend(["-d", pg_db])

    if pg_user is not None:
        cmd.extend(["-U", pg_user])

    if pg_host is not None:
        cmd.extend(["-h", pg_host])

    if pg_port is not None:
        cmd.extend(["-p", pg_port])

    cmd.extend([
        "-X",
        "--csv",
        "-f",
        fname,
    ])

    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
# 1. sort
# 2. remove the end     
def format_csv_content(content: str, delete_end_new_line: bool):
    array = content.splitlines()
    header = array[0]
    rows = array[1:]
    if len(rows) == 0:
        return content

    # print(rows)
    if delete_end_new_line and rows[len(rows)-1] == b'':
        rows = rows[:(len(rows) - 1)]  

    rows.sort()
    # 拼接成str
    result = b'\n'.join(rows)
    return header + b'\n' + result + b'\n'

def is_numpy_floating(dtype):
    np.issubdtype(dtype, np.floating) 

def both_empty_result(c1 :str, c2 :str)->bool:
    len(c1) == 0 and pd.read_csv(io.BytesIO(c2), keep_default_na=False).empty

# root = Path(os.path.dirname(__file__)) / "sqls"
root = Path(os.getcwd()) / "testdata/sqls"
# print(datafusion_cli)
test_files = set(root.glob("*.sql"))
# for file in test_files:
#     print(file)

class TestPsqlParity:
    def test_tests_count(self):
        assert len(test_files) == 3, "tests are missed"

    @pytest.mark.parametrize("fname", test_files, ids=str)
    def test_sql_file(self, fname):
        out1 = generate_csv_from_datafusion(fname)
        if out1.returncode != 0 or out1.stderr != b'':
           pytest.fail(f"datafusion-cli cmd out:{out1.stderr}")

        out2 = generate_csv_from_psql(fname)
        if out2.returncode != 0 or out2.stderr != b'':
           pytest.fail(f"psql cmd out:{out2.stderr}")

        if both_empty_result(out1.stdout, out2.stdout):    
           f_r1 = format_csv_content(out1.stdout, True)
           f_r2 = format_csv_content(out2.stdout, False)
           df1 = pd.read_csv(io.BytesIO(f_r1), keep_default_na=False)
           df2 = pd.read_csv(io.BytesIO(f_r2), keep_default_na=False)

           # TODO error message
           for column_name in df1.columns:
               c1 = df1[column_name].to_numpy()
               c2 = df2[column_name].to_numpy()
               if is_numpy_floating(c1.dtype):
                  np.testing.assert_allclose(c1, c2, equal_nan=True, verbose=True)
               else:
                  np.testing.assert_equal(c1,c2, verbose=True)

# print("hello world")
# print(test_files)
# print(generate_csv_from_datafusion("/home/work/arrow-datafusion/integration-tests/sqls/simple_group_by.sql"))
# print(generate_csv_from_psql("/home/work/arrow-datafusion/integration-tests/sqls/simple_group_by.sql"))

# t1 = b'c2,sum_c3,avg_c3,max_c3,min_c3,count_c3\n1,367,16.681818181818183,125,-99,22\n2,184,8.363636363636363,122,-117,22\n3,395,20.789473684210527,123,-101,19\n4,29,1.2608695652173914,123,-117,23\n5,-194,-13.857142857142858,118,-101,14\n\n'.splitlines()
# t1.sort()
# print(t1)

# r1 = generate_csv_from_datafusion("./testdata/query.sql")
# r2 = generate_csv_from_psql("./testdata/query.sql")

# print(r1)
# print(r2)

# f_r1 = format_csv_content(r1, True)
# f_r2 = format_csv_content(r2, False)

# print(f_r1)
# print(f_r2)

# df1 = pd.read_csv(io.BytesIO(r1), keep_default_na=False)
# df2 = pd.read_csv(io.BytesIO(r2), keep_default_na=False)

# print(df1)
# print(df2)

# # TODO error message
# for column_name in df1.columns:
#     c1 = df1[column_name].to_numpy()
#     c2 = df2[column_name].to_numpy()
#     if is_numpy_floating(c1.dtype):
#         print("float")
#         np.testing.assert_allclose(c1, c2, equal_nan=True, verbose=True)
#     else:
#         print("not float")    
#         np.testing.assert_equal(c1,c2, verbose=True)

# print("hello")

# try:
#    r1 = generate_csv_from_datafusion("./testdata/sqls/query1.sql")
# except subprocess.CalledProcessError as e:
#     print("datafusion raise: {}".format(), e.output)
#     print("hello")

# try:
# cmd = generate_csv_from_psql_cmd("./testdata/sqls/query1.sql")
# out = subprocess.run(cmd, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
# out = generate_csv_from_datafusion("./testdata/sqls/query1.sql")
# print(out)
# if out.returncode != 0 or out.stderr != b'':
#     print(f"{out.stderr}")
    # pytest.fail(f"psql cmd out:{out}")
# except subprocess.CalledProcessError as e:
#     h = "hello"
#     print(h)
#     print(e.output)   

# try:
#     raise Exception('spam', 'eggs')
# except Exception as e:
#     print(e)    

# if both_empty_result(r1, r2):    
#   # 如果r1 r2为空，则返回成功
#   f_r1 = format_csv_content(r1, True)
#   f_r2 = format_csv_content(r2, False)
#   df1 = pd.read_csv(io.BytesIO(f_r1), keep_default_na=False)
#   df2 = pd.read_csv(io.BytesIO(f_r2), keep_default_na=False)

#   # TODO error message
#   for column_name in df1.columns:
#     c1 = df1[column_name].to_numpy()
#     c2 = df2[column_name].to_numpy()
#     if is_numpy_floating(c1.dtype):
#        np.testing.assert_allclose(c1, c2, equal_nan=True, verbose=True)
#     else:
#        np.testing.assert_equal(c1,c2, verbose=True)




