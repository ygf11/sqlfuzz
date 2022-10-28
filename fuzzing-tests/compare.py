import io
import os
import subprocess
from pathlib import Path
from wsgiref import headers

import numpy as np
import pandas as pd
import pytest

pg_db, pg_user, pg_host, pg_port = [
    os.environ.get(i)
    for i in (
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
    )
]

CREATE_TABLE_SQL_FILE = "./fuzzing-tests/create_table.sql"


def generate_csv_from_datafusion(fname: str):
    return subprocess.check_output(
        [
            "./arrow-datafusion/datafusion-cli/target/debug/datafusion-cli",
            "-f",
            CREATE_TABLE_SQL_FILE,
            "-f",
            fname,
            "--format",
            "csv",
            "-q",
        ],
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

    return subprocess.check_output(cmd)
# 1. sort
# 2. rmove the end     
def format_csv_content(content: str, delete_end_new_line: bool):
    array = content.splitlines()
    header = array[0]
    rows = array[1:]

    # print(rows)
    if delete_end_new_line and rows[len(rows)-1] == b'':
        rows = rows[:(len(rows) - 1)]  

    rows.sort()
    # 拼接成str
    result = b'\n'.join(rows)
    return header + b'\n' + result + b'\n'


root = Path(os.path.dirname(__file__)) / "sqls"
test_files = set(root.glob("*.sql"))


# class TestPsqlParity:
#     def test_tests_count(self):
#         assert len(test_files) == 25, "tests are missed"

#     @pytest.mark.parametrize("fname", test_files, ids=str)
#     def test_sql_file(self, fname):
#         df_fo = open("datafusion.out", "w")
#         df_fo.write(io.BytesIO(generate_csv_from_datafusion(fname)))
#         df_fo.close()
#         datafusion_output = pd.read_csv(io.BytesIO(generate_csv_from_datafusion(fname)))
#         pg_fo = open("pg.out", "w")
#         pg_fo.write(io.BytesIO(generate_csv_from_psql(fname)))
#         pg_fo.close()
#         psql_output = pd.read_csv(io.BytesIO(generate_csv_from_psql(fname)))
#         np.testing.assert_allclose(datafusion_output, psql_output, equal_nan=True, verbose=True)

# print("hello world")
# print(test_files)
# print(generate_csv_from_datafusion("/home/work/arrow-datafusion/integration-tests/sqls/simple_group_by.sql"))
# print(generate_csv_from_psql("/home/work/arrow-datafusion/integration-tests/sqls/simple_group_by.sql"))

# t1 = b'c2,sum_c3,avg_c3,max_c3,min_c3,count_c3\n1,367,16.681818181818183,125,-99,22\n2,184,8.363636363636363,122,-117,22\n3,395,20.789473684210527,123,-101,19\n4,29,1.2608695652173914,123,-117,23\n5,-194,-13.857142857142858,118,-101,14\n\n'.splitlines()
# t1.sort()
# print(t1)

r1 = generate_csv_from_datafusion("./fuzzing-tests/query.sql")
r2 = generate_csv_from_psql("./fuzzing-tests/query.sql")

# print(r1)
# print(r2)

# t1 = format_csv_content(b'c2,sum_c3,avg_c3,max_c3,min_c3,count_c3\n1,367,16.681818181818183,125,-99,22\n2,184,8.363636363636363,122,-117,22\n3,395,20.789473684210527,123,-101,19\n4,29,1.2608695652173914,123,-117,23\n5,-194,-13.857142857142858,118,-101,14\n\n', True)
# t2 = format_csv_content(b'c2,sum_c3,avg_c3,max_c3,min_c3,count_c3\n2,184,8.3636363636363636,122,-117,22\n3,395,20.7894736842105263,123,-101,19\n4,29,1.2608695652173913,123,-117,23\n5,-194,-13.8571428571428571,118,-101,14\n1,367,16.6818181818181818,125,-99,22\n', False)

f_r1 = format_csv_content(r1)
f_r2 = format_csv_content(r2)
# df1 = pd.read_csv(f_r1)
# df2 = pd.read_csv(f_r2)
# np.testing.assert_allclose(df1, df2, equal_nan=True)


