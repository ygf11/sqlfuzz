-- SQL Query #1:

SELECT _c0, _c1, _c2, _c3, _c4, _c5
FROM (SELECT test0.c0 AS _c0, test0.c1 AS _c1, test0.c2 AS _c2, test0.c3 AS _c3, test0.c4 AS _c4, test0.c5 AS _c5
  FROM (test0))
WHERE _c3 != _c0;